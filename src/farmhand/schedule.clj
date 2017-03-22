(ns farmhand.schedule
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as q]
            [farmhand.redis :as r :refer [with-jedis with-transaction]]
            [farmhand.utils :refer [now-millis safe-loop]])
  (:import (redis.clients.jedis Jedis Transaction)))

(defn schedule-key ^String [queue-name] (r/redis-key "schedule:" queue-name))

(defn run-at*
  [^Transaction transaction job-id queue-name at]
  (.zadd transaction (schedule-key queue-name) (double at) ^String job-id)
  (jobs/update-props transaction job-id {:status "scheduled"}))

(defn run-at
  "Schedules a job to run at some time in the future. See the docs in
  farmhand.core/run-at for more details."
  [pool job at]
  (let [{job-id :job-id queue-name :queue :as normalized} (jobs/normalize job)]
    (with-transaction pool transaction
      (jobs/save-new transaction normalized)
      (run-at* transaction job-id queue-name at))
    job-id))

(def ^:private multipliers {:milliseconds 1
                            :seconds 1000
                            :minutes (* 1000 60)
                            :hours (* 1000 60 60)
                            :days (* 1000 60 60 24)})

(defn from-now
  [n unit]
  {:pre [(get multipliers unit)]}
  (let [multiplier (get multipliers unit)]
    (+ (now-millis) (* n multiplier))))

(defn run-in
  "Schedules a job to run at some time relative to now. See the docs in
  farmhand.core/run-in for more details."
  [pool job n unit]
  (run-at pool job (from-now n unit)))

(defn- fetch-ready-id
  "Fetches the next job-id that is ready to be enqueued (or nil if there is
  none)."
  [^Jedis jedis queue-name ^String now]
  (-> (.zrangeByScore jedis (schedule-key queue-name) "-inf" now (int 0) (int 1))
      (first)))

(defn pull-and-enqueue
  [pool queues]
  (let [now-str (str (now-millis))]
    (with-jedis pool jedis
      (doseq [{queue-name :name} queues
              :let [sch-key (schedule-key queue-name)]]
        (loop []
          (.watch jedis (r/str-arr sch-key))
          (if-let [job-id (fetch-ready-id jedis queue-name now-str)]
            (let [transaction (.multi jedis)]
              (.zrem transaction sch-key (r/str-arr job-id))
              (q/push transaction job-id queue-name)
              (.exec transaction)
              (recur))
            (.unwatch jedis)))))))

(defn- sleep-time [] (int (* (rand 15) 1000)))

(defn schedule-thread
  [pool stop-chan queues]
  (async/thread
    (log/info "in schedule thread")
    (safe-loop
      (async/alt!!
        stop-chan :exit-loop
        (async/timeout (sleep-time)) (pull-and-enqueue pool queues)))
    (log/info "exiting schedule thread")))
