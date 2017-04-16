(ns farmhand.handler
  (:require [clojure.tools.logging :as log]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as queue]
            [farmhand.redis :as r :refer [with-transaction]]
            [farmhand.registry :as registry]
            [farmhand.retry :as retry]
            [farmhand.schedule :as schedule]
            [farmhand.utils :refer [from-now now-millis rethrow-if-fatal]]))

(defn execute-job
  "Executes the job's function with the defined arguments."
  [{{fn-var :fn-var args :args} :job :as request}]
  (-> request
      (update-in [:job] assoc
                 :result (:farmhand/result (apply fn-var args))
                 :status "complete"
                 :completed-at (now-millis))
      (assoc :registry queue/completed-registry)))

(defn- update-for-failure
  [{{:keys [job-id queue] :as job} :job :as request} exception]
  (if-let [{:keys [delay-time delay-unit] :as retry} (retry/update-retry job)]
    (assoc request
           :job (assoc job :retry retry)
           :registry (schedule/registry-name queue)
           :registry-opts {:expire-at (from-now delay-time delay-unit)})
    (assoc request
           :job (assoc job
                       :status "failed"
                       :reason (str exception)
                       :failed-at (now-millis))
           :registry queue/dead-letter-registry)))

(defn- handle-response
  [{:keys [context job registry registry-opts] :as response}]
  (queue/finish-execution context job registry registry-opts)
  response)

(defn wrap-outer
  "Middleware which performs the basics of the job flow. It marks the job as in
  progress, fetches the body of the job from Redis, and executes the given
  handler. Then handles the response by either marking the job as failed or
  completed.

  If the response map contains a truthy value for the :handled? key, then the
  job will not be marked as either failed or success. It assumes some other
  middleware took care of that."
  [handler]
  (fn outer [{:keys [{:keys [job-id] :as job} context] :as request}]
    (handle-response
      (try
        (handler request)
        (catch Throwable e
          (rethrow-if-fatal e)
          (log/infof e "job-id (%s) threw an exception" job-id)
          (update-for-failure request e))))))

(def default-handler (wrap-outer execute-job))
