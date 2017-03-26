(ns farmhand.handler
  (:require [clojure.tools.logging :as log]
            [farmhand.dead-letters :as dead-letters]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as queue]
            [farmhand.retry :refer [wrap-retry]]
            [farmhand.utils :refer [fatal?]]))

(defn- handle-failure
  [context job-id {:keys [reason exception]}]
  (case reason
    :malformed-job
    (do
      (log/infof "The body of this job (%s) is malformed." job-id)
      (dead-letters/fail context job-id :reason "Malformed: Job definition is invalid"))

    :no-implementation
    (do
      (log/info "Job cannot be processed - there is no implementation" job-id)
      (dead-letters/fail context job-id :reason "Unknown job type"))

    :exception
    (do
      (log/infof exception "While processing job (%s)" job-id)
      (dead-letters/fail context job-id :reason (str exception)))))

(defn- handle-success
  [context job-id result]
  (queue/complete context job-id :result result))

(defn- fetch-job
  [{:keys [job-id context] :as request}]
  (assoc request
         :job (->> job-id
                   (jobs/fetch-body context)
                   jobs/assoc-fn-var)))

(defn- mark-in-progress
  [{:keys [job-id context] :as request}]
  (jobs/update-props context job-id {:status "processing"})
  request)

(defn- handle-response
  [{:keys [job-id context]} {:keys [status result handled?] :as response}]
  (when-not handled?
    (case status
      :failure (handle-failure context job-id result)
      :success (handle-success context job-id result)))
  response)

(defn execute-job
  "Executes the job's function with the defined arguments. If the function
  cannot be found, returns a failed response where the reason is
  :no-implementation.

  This function does not handle any exceptions. You must use this in
  conjunction with wrap-exception-handler or with your own exception handler
  middleware."
  [{{fn-var :fn-var args :args} :job}]
  (if fn-var
    {:status :success
     :result (apply fn-var args)}
    {:status :failure
     :result {:reason :no-implementation}}))

(defn wrap-exception-handler
  "Middleware that catches exceptions. Relies on the farmhand.utils/fatal?
  function to define whether an exception can be handled. If an exception is
  considered fatal, then the exception is rethrown."
  [handler]
  (fn exception-handler [request]
    (try
      (handler request)
      (catch Throwable e
        (when (fatal? e) (throw e))
        {:status :failure
         :result {:reason :exception :exception e}}))))

(defn wrap-outer
  "Middleware which performs the basics of the job flow. It marks the job as in
  progress, fetches the body of the job from Redis, and executes the given
  handler. Then handles the response by either marking the job as failed or
  completed.

  If the response map contains a truthy value for the :handled? key, then the
  job will not be marked as either failed or success. It assumes some other
  middleware took care of that."
  [handler]
  ;; Originally this function was split into a few different middlewares:
  ;;  wrap-mark-in-progress
  ;;  wrap-fetch-job
  ;;  wrap-handle-response
  ;; But I'm not sure there is really a use for this. The code is simpler to
  ;; just have one outer wrapper. If the use case ever comes up for splitting
  ;; it, then it can be revisited.
  (fn outer [request]
    (let [request_ (-> request
                       mark-in-progress
                       fetch-job)
          response (handler request_)]
      (handle-response request_ response))))

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
