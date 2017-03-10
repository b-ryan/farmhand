(ns farmhand.dead-letters
  (:require [farmhand.jobs :as jobs]
            [farmhand.queue :as queue]
            [farmhand.redis :as r :refer [with-jedis]]
            [farmhand.registry :as registry]
            [farmhand.utils :refer [now-millis]]))

(defn dead-letter-key ^String [] (r/redis-key "dead"))

(defn requeue
  [job-id pool]
  (with-jedis pool jedis
    (let [{:keys [queue]} (jobs/fetch-body* job-id jedis)
          transaction (.multi jedis)]
      (registry/add transaction (dead-letter-key) job-id)
      (queue/push transaction job-id queue)
      (.exec transaction))))

(defn fail
  [job-id pool & {:keys [reason]}]
  (with-jedis pool jedis
    (let [transaction (.multi jedis)]
      (registry/delete transaction (queue/in-flight-key) job-id)
      (registry/add transaction (dead-letter-key) job-id)
      (jobs/update-props transaction job-id {:status "failed"
                                             :reason reason
                                             :failed-at (now-millis)})
      (.exec transaction))))
