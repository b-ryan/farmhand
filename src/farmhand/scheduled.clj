(ns farmhand.scheduled
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as q]
            [farmhand.redis :as r :refer [with-jedis with-transaction]]
            [farmhand.utils :refer [now-millis safe-loop]])
  (:import (redis.clients.jedis Jedis)))

(defn scheduled-key ^String [] (r/redis-key "scheduled"))

(defn run-at
  "at should be a millisecond timestamp"
  [job at pool]
  (let [{job-id :job-id :as normalized} (jobs/normalize job)]
    (with-transaction pool transaction
      (jobs/save-new transaction normalized)
      (.zadd transaction (scheduled-key) (double at) ^String job-id))))

(defn- fetch-ready-id
  "Fetches the next job-id that is ready to be enqueued (or nil if there is
  none)."
  [^Jedis jedis ^String now]
  (-> (.zrangeByScore jedis (scheduled-key) "-inf" now (int 0) (int 1))
      (first)))

(defn pull-and-enqueue
  [pool]
  (let [now-str (str (now-millis))]
    (with-jedis pool jedis
      (loop []
        (.watch jedis (r/str-arr (scheduled-key)))
        (if-let [job-id (fetch-ready-id jedis now-str)]
          (let [{:keys [queue]} (jobs/fetch-body* job-id jedis)
                transaction (.multi jedis)]
            (.zrem transaction (scheduled-key) (r/str-arr job-id))
            (q/push transaction job-id queue)
            (.exec transaction)
            (recur))
          (.unwatch jedis))))))

(defn- sleep-time
  []
  ;; TODO mimick sidekiq
  ;; https://github.com/mperham/sidekiq/blob/master/lib/sidekiq/scheduled.rb
  (int (* (rand 15) 1000)))

(defn schedule-loop
  [pool stop-chan]
  (log/info "In schedule loop")
  (safe-loop
    (async/alt!!
      stop-chan :exit-loop
      (async/timeout (sleep-time)) (pull-and-enqueue pool)))
  (log/info "Exiting schedule loop"))
