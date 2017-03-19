(ns farmhand.retry
  (:require [farmhand.jobs :as jobs]
            [farmhand.redis :refer [with-transaction]]
            [farmhand.schedule :as schedule]))

(defmulti update-retry
  "Multimethod which takes a job containing a retry map and returns a new retry
  map. A return value of nil indicates the job should not be retried."
  (comp :strategy :retry))

(defmethod update-retry nil [_] nil)

;; 0   1   2   3   4    5    6    7    =  8 attempts
;; 1 + 2 + 4 + 8 + 16 + 32 + 64 + 128  =  ~10 days
(def ^:private backoff-defaults
  {:num-attempts 0
   :max-attempts 8
   :coefficient 1
   :exp-base 2
   :delay-unit :hours})

(defmethod update-retry "backoff"
  ;; A retry strategy that uses an exponential backoff with a maximum number
  ;; of retries. By default, will cause the first retry to occur in roughly 1
  ;; hour, the next in 2 hours, the next in 4 hours, etc. until the maximum
  ;; number of attempts has occurred.
  [{:keys [retry] :as job}]
  (let [retry (merge backoff-defaults retry)
        {:keys [num-attempts max-attempts coefficient exp-base]} retry
        min-delay-time (* coefficient (Math/pow exp-base num-attempts))]
    (when (< (inc num-attempts) max-attempts)
      (assoc retry
             :num-attempts (inc num-attempts)
             :delay-time (int (+ min-delay-time (rand min-delay-time)))))))

(defn- handle-retry
  [{{:keys [job-id queue] :as job} :job pool :pool} response]
  (let [{:keys [delay-time delay-unit] :as retry} (update-retry job)]
    (with-transaction pool transaction
      (jobs/update-props transaction job-id {:retry retry})
      (if retry
        (let [delay-ts (schedule/from-now delay-time delay-unit)]
          (schedule/run-at* transaction job-id queue delay-ts)
          (assoc response :handled? true))
        response))))

(defn wrap-retry
  "Middleware for automatically scheduling jobs to be retried. This middleware
  should be sandwiched somewhere between
  farmhand.handler/execute-with-ex-handle and farmhand.handler/wrap-outer.

  See farmhand.handler/default-handler for an example."
  [handler]
  (fn [request]
    (let [{:keys [status handled?] :as response} (handler request)]
      (if (and (= status :failure) (not handled?))
        (handle-retry request response)
        response))))
