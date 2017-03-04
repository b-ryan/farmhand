(ns farmhand.work
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [farmhand.dead-letters :as dead-letters]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as queue]
            [farmhand.redis :refer [with-jedis]]
            [farmhand.utils :refer [fatal? safe-loop]]))

(def ^:private no-jobs-sleep-ms 50)

(defn- handle-failure
  [job-id pool {:keys [reason exception]}]
  (case reason
    :malformed-job
    (do
      (log/infof "The body of this job (%s) is malformed." job-id)
      (dead-letters/fail job-id pool :reason "Malformed: Job definition is invalid"))

    :no-implementation
    (do
      (log/info "Job cannot be processed - there is no implementation" job-id)
      (dead-letters/fail job-id pool :reason "Unknown job type"))

    :exception
    (do
      (log/infof exception "While processing job (%s)" job-id)
      (dead-letters/fail job-id pool :reason (str exception)))))

(defn- handle-success
  [job-id pool result]
  (queue/complete job-id pool :result result))

(defn handler
  [{{fn-var :fn-var args :args} :job}]
  (if fn-var
    {:status :success
     :result (apply fn-var args)}
    {:status :failure
     :result {:reason :no-implementation}}))

(defn wrap-exception-handler
  [handler]
  (fn exception-handler [request]
    (try
      (handler request)
      (catch Throwable e
        (when (fatal? e) (throw e))
        {:status :failure
         :result {:reason :exception
                  :exception e}}))))

(defn wrap-fetch-job
  [handler]
  (fn fetch-job [{:keys [job-id pool] :as request}]
    (handler (assoc request
                    :job (-> job-id
                             (jobs/fetch-body pool)
                             jobs/assoc-fn-var)))))

(defn wrap-mark-in-progress
  [handler]
  (fn mark-in-progress [{:keys [job-id pool] :as request}]
    (with-jedis pool jedis
      (let [pipeline (.pipelined jedis)]
        (jobs/update-props pipeline job-id {:status "processing"})
        (.sync pipeline)))
    (handler request)))

(defn wrap-handle-response
  [handler]
  (fn handle-response [{:keys [job-id pool] :as request}]
    (let [{:keys [status result] :as response} (handler request)]
      (case status
        :failure (handle-failure job-id pool result)
        :success (handle-success job-id pool result))
      response)))

(defn wrap-debug
  [handler]
  (fn debug [{:keys [job-id] :as request}]
    (log/debugf "received job %s" job-id)
    (let [response (handler request)]
      (log/debugf "completed job %s" job-id)
      response)))

(def default-handler (-> handler
                         wrap-exception-handler
                         wrap-fetch-job
                         wrap-mark-in-progress
                         wrap-handle-response
                         wrap-debug))

(defn run-once
  [pool queue-defs]
  (if-let [job-id (->> (queue/queue-order queue-defs)
                       (queue/dequeue pool))]
    (default-handler {:job-id job-id :pool pool})
    ::no-jobs-available))

(defn- sleep-if-no-jobs
  [result]
  ;; When there are no jobs, sleep for a short period of time to avoid pounding
  ;; Redis. This could be avoided if
  ;; https://github.com/antirez/redis/issues/1785 were implemented.
  (when (= result ::no-jobs-available)
    (Thread/sleep no-jobs-sleep-ms)))

(defn main-loop
  [pool stop-chan queue-defs]
  (log/info "in main loop" queue-defs)
  (safe-loop
    (async/alt!!
      stop-chan :exit-loop
      :default (-> (run-once pool queue-defs)
                   (sleep-if-no-jobs))))
  (log/info "exiting main loop"))
