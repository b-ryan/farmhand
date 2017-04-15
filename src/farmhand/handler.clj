(ns farmhand.handler
  (:require [clojure.tools.logging :as log]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as queue]
            [farmhand.redis :as r :refer [with-transaction]]
            [farmhand.registry :as registry]
            [farmhand.retry :as retry]
            [farmhand.schedule :as schedule]
            [farmhand.utils :refer [now-millis rethrow-if-fatal]]))

(defn execute-job
  "Executes the job's function with the defined arguments."
  [{{fn-var :fn-var args :args} :job :as request}]
  (-> request
      (update-in [:job] assoc
                 :result (:farmhand/result (apply fn-var args))
                 :status "complete"
                 :completed-at (now-millis))
      (assoc :registry queue/completed-registry)))

(defn- handle-exception
  [{{:keys [job-id queue] :as job} :job :as request} exception]
  (log/infof exception "job-id (%s) threw an exception" job-id)
  (if-let [{:keys [delay-time delay-unit] :as retry} (retry/update-retry job)]
    (assoc request
           :job (assoc job :retry retry)
           :registry (schedule/registry-name queue)
           :registry-opts {:expire-at (schedule/from-now delay-time delay-unit)})
    (assoc request
           :job (assoc job
                       :status "failed"
                       :reason (str exception)
                       :failed-at (now-millis))
           :registry queue/dead-letter-registry)))

(defn- finish-execution
  [{:keys [context {job-id :job-id :as job} registry registry-opts] :as response}]
  (with-transaction [context context]
    (registry/delete context job-id queue/in-flight-registry)
    (registry/add context job-id registry registry-opts)
    (jobs/save context job))
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
  (fn outer [{:keys [job-id context] :as request}]
    (finish-execution
      (try
        (-> request (assoc :job (jobs/fetch context job-id)) handler)
        (catch Throwable e
          (rethrow-if-fatal e)
          (handle-exception request e))))))

(def default-handler (wrap-outer execute-job))
