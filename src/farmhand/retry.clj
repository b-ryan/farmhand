(ns farmhand.retry
  (:require [clojure.tools.logging :as log]
            [farmhand.jobs :as jobs]
            [farmhand.redis :refer [with-transaction]]
            [farmhand.queue :as queue]
            [farmhand.registry :as registry]
            [farmhand.schedule :as schedule]
            [farmhand.utils :refer [from-now now-millis rethrow-if-fatal]]))

(defmulti update-retry
  "Multimethod which takes a job containing a retry map and returns a new job
  with the retry key updated. When the :retry is set to nil, the job will not
  be retried."
  (comp :strategy :retry))

(defmethod update-retry nil [job] (assoc job :retry nil))

;; 0   1   2   3   4    5    6    7    =  8 attempts
;; 1 + 2 + 4 + 8 + 16 + 32 + 64 + 128  =  ~10 days
(def ^:private backoff-defaults
  {:num-attempts 0
   :max-attempts 8
   :coefficient 1
   :delay-unit :hours})

(defmethod update-retry "backoff"
  ;; A retry strategy that uses an exponential backoff with a maximum number
  ;; of retries. By default, will cause the first retry to occur in roughly 1
  ;; hour, the next in 2 hours, the next in 4 hours, etc. until the maximum
  ;; number of attempts has occurred.
  [{:keys [retry] :as job}]
  (let [retry (merge backoff-defaults retry)
        {:keys [num-attempts max-attempts coefficient]} retry
        delay-time (* coefficient (Math/pow 2 num-attempts))]
    ;; TODO add randomness
    (assoc job :retry
           (when (< (inc num-attempts) max-attempts)
             (assoc retry
                    :num-attempts (inc num-attempts)
                    :delay-time (int delay-time))))))

(defn- handle-retry
  [{{:keys [job-id queue retry] :as job} :job context :context :as response}]
  (if retry
    (with-transaction [context context]
      (let [{:keys [delay-time delay-unit]} retry
            run-at-time (from-now delay-time delay-unit)
            job (schedule/run-at context job run-at-time)]
        (registry/delete context job-id queue/in-flight-registry)
        (assoc response :job job :handled? true)))
    response))

(defn wrap-retry
  "Middleware for automatically scheduling jobs to be retried."
  [handler]
  (fn [request]
    (let [{:keys [exception handled?] :as response} (handler request)]
      (if (and exception (not handled?))
        (-> response
            (update-in [:job] update-retry)
            handle-retry)
        response))))
