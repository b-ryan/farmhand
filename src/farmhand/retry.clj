(ns farmhand.retry
  (:require [farmhand.jobs :as jobs]
            [farmhand.redis :refer [with-transaction]]
            [farmhand.schedule :as schedule]))

(defmulti update-retry
  "Multimethod which takes a job containing a retry map and returns a new retry
  map. A return value of nil indicates the job should not be retried."
  (comp :strategy :retry))

(defmethod update-retry nil [_] nil)

(def ^:private backoff-defaults
  {:num-attempts 0
   :coefficient 1
   :exp-base 2
   :delay-unit :hours})

(defmethod update-retry "backoff"
  [{:keys [retry] :as job}]
  ;; TODO Implement either a max # of tries or max amount of time to retry.
  (let [retry (merge backoff-defaults retry)
        {:keys [num-attempts coefficient exp-base]} retry
        min-delay-time (* coefficient (Math/pow exp-base num-attempts))]
    (assoc retry
           :num-attempts (inc num-attempts)
           :delay-time (+ min-delay-time (rand min-delay-time)))))

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
