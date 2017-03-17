(ns farmhand.work
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [farmhand.queue :as queue]
            [farmhand.utils :refer [safe-loop]]))

(def ^:private no-jobs-sleep-ms 50)

(defn run-once
  [pool queues handler]
  (if-let [job-id (->> (queue/queue-order queues)
                       (queue/dequeue pool))]
    (handler {:job-id job-id :pool pool})
    ::no-jobs-available))

(defn- sleep-if-no-jobs
  [result]
  ;; When there are no jobs, sleep for a short period of time to avoid pounding
  ;; Redis. This could be avoided if
  ;; https://github.com/antirez/redis/issues/1785 were implemented.
  (when (= result ::no-jobs-available)
    (Thread/sleep no-jobs-sleep-ms)))

(defn work-thread
  [pool stop-chan queues handler]
  (async/thread
    (log/info "in main loop" queues)
    (safe-loop
      (async/alt!!
        stop-chan :exit-loop
        :default (-> (run-once pool queues handler)
                     (sleep-if-no-jobs))))
    (log/info "exiting main loop")))
