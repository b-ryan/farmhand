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
  [{{fn-var :fn-var args :args job-id :job-id} :job :as request}]
  (try
    (update-in request [:job] assoc :result (:farmhand/result (apply fn-var args)))
    (catch Throwable e
      (rethrow-if-fatal e)
      (log/infof e "job-id (%s) threw an exception" job-id)
      (assoc request :exception e))))

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
      (if-not handled?
        (if exception
          (queue/fail context (assoc job :reason (str exception)))
          (queue/complete context job))
        response))))

(def default-handler (-> execute-job retry/wrap-retry wrap-outer))
