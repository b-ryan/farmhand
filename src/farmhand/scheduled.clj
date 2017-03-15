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
  "Schedules a job to be run at a specified time. Farmhand will queue the job
  roughly when it is scheduled, but currently does not guarantee accuracy.

  The 'at' argument should be a timestamp specified in milliseconds.

  Returns the job's ID."
  [pool job at]
  (let [{job-id :job-id :as normalized} (jobs/normalize job)]
    (with-transaction pool transaction
      (jobs/save-new transaction normalized)
      (.zadd transaction (scheduled-key) (double at) ^String job-id))
    job-id))

(def ^:private multipliers {:milliseconds 1
                            :seconds 1000
                            :minutes (* 1000 60)
                            :hours (* 1000 60 60)
                            :days (* 1000 60 60 24)})

(defn run-in
  "Schedules a job to be run at some time from now. Like run-at, the job will
  be queued roughly around the requested time.

  The 'unit' argument can be one of :milliseconds, :seconds, :minutes, :hours,
  or :days and specifies the unit of 'in'. For example, schedule a job in 2
  minutes with

    (run-in pool job 2 :minutes)

  Returns the job's ID."
  [pool job in unit]
  {:pre [(get multipliers unit)]}
  (let [multiplier (get multipliers unit)
        at (+ (now-millis) (* in multiplier))]
    (run-at pool job at)))

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

(defn- sleep-time [] (int (* (rand 15) 1000)))

(defn schedule-thread
  [pool stop-chan]
  (async/thread
    (log/info "in schedule thread")
    (safe-loop
      (async/alt!!
        stop-chan :exit-loop
        (async/timeout (sleep-time)) (pull-and-enqueue pool)))
    (log/info "exiting schedule thread")))
