(ns farmhand.handler
  (:require [clojure.tools.logging :as log]
            [farmhand.queue :as queue]
            [farmhand.redis :refer [with-transaction]]
            [farmhand.registry :as registry]
            [farmhand.retry :as retry]
            [farmhand.utils :refer [from-now rethrow-if-fatal]]))

(defn execute-job
  "Executes the job's function with the defined arguments."
  [{{fn-var :fn-var args :args job-id :job-id} :job :as request}]
  (try
    (update-in request [:job] assoc :result (:farmhand/result (apply fn-var args)))
    (catch Throwable e
      (rethrow-if-fatal e)
      (log/infof e "job-id (%s) threw an exception" job-id)
      (assoc request :exception e))))

(defn- handle-retry
  [{{:keys [job-id queue retry] :as job} :job context :context :as response}]
  (if (:state retry)
    (with-transaction [context context]
      (let [{:keys [delay-time delay-unit]} (:state retry)
            run-at-time (from-now delay-time delay-unit)
            job (queue/run-at context job run-at-time)]
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
            (update-in [:job] retry/update-job)
            handle-retry)
        response))))

(defn wrap-outer
  "Middleware which performs the basics of the job flow. It marks the job as in
  progress, fetches the body of the job from Redis, and executes the given
  handler. Then handles the response by either marking the job as failed or
  completed.

  If the response map contains a truthy value for the :handled? key, then the
  job will not be marked as either failed or success. It assumes some other
  middleware took care of that."
  [handler]
  (fn outer [request]
    (let [{:keys [context job exception handled?] :as response} (handler request)]
      (cond
        handled? response
        exception (queue/fail context (assoc job :reason (str exception)))
        :else (queue/complete context job)))))

(def default-handler (-> execute-job wrap-retry wrap-outer))
