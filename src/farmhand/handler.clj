(ns farmhand.handler
  (:require [clojure.tools.logging :as log]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as queue]
            [farmhand.retry :refer [wrap-retry]]
            [farmhand.utils :refer [fatal?]]))

(defn execute-job
  "Executes the job's function with the defined arguments. If the function
  cannot be found, returns a failed response where the reason is
  :no-implementation.

  This function does not handle any exceptions. You must use this in
  conjunction with wrap-exception-handler or with your own exception handler
  middleware."
  [{{fn-var :fn-var args :args} :job :as request}]
  (merge request
         (if fn-var
           {:status :success :result (apply fn-var args)}
           {:status :failure :reason "Function cannot be found"})))

(defn wrap-exception-handler
  "Middleware that catches exceptions. Relies on the farmhand.utils/fatal?
  function to define whether an exception can be handled. If an exception is
  considered fatal, then it is rethrown."
  [handler]
  (fn exception-handler [{:keys [job-id] :as request}]
    (try
      (handler request)
      (catch Throwable e
        (when (fatal? e) (throw e))
        (log/infof e "Job threw an exception. job-id: (%s)" job-id)
        (assoc request
               :status :failure
               :reason (str e)
               :exception e)))))

(defn- fetch-job
  [{:keys [job-id context] :as request}]
  (assoc request :job (jobs/fetch context job-id)))

(defn- mark-in-progress
  [{:keys [job context] :as request}]
  (assoc request :job (jobs/update-props context job {:status "processing"})))

(defn- handle-response
  [{:keys [job context status result reason handled?] :as response}]
  (if-not handled?
    (assoc
      response :job
      (case status
        :failure (queue/fail context job :reason reason)
        :success (queue/complete context job :result result)))
    response))

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
    (-> request
        fetch-job
        mark-in-progress
        handler
        handle-response)))

(defn wrap-debug
  "Utility function provided for convenience. Logs the request and response."
  [handler]
  (fn debug [request]
    (log/debugf "received request %s" request)
    (let [response (handler request)]
      (log/debugf "received response %s" response)
      response)))

(def default-handler (-> execute-job
                         wrap-exception-handler
                         wrap-retry
                         wrap-outer))
