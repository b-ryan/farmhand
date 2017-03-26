(ns farmhand.dead-letters
  (:require [farmhand.jobs :as jobs]
            [farmhand.queue :as queue]
            [farmhand.redis :as r :refer [with-jedis* with-transaction*]]
            [farmhand.registry :as registry]
            [farmhand.utils :refer [now-millis]])
  (:import (redis.clients.jedis Jedis Transaction)))

(defn dead-letter-key ^String [] (r/redis-key "dead"))

(defn requeue
  [context job-id]
  (with-jedis* [{:keys [jedis] :as context} context]
    (let [{:keys [queue]} (jobs/fetch-body context job-id)]
      (with-transaction* [{:keys [transaction] :as context} context]
        (registry/add transaction (dead-letter-key) job-id)
        (queue/push context job-id queue)))))

(defn fail
  [context job-id & {:keys [reason]}]
  (with-transaction* [{:keys [transaction] :as context} context]
    (registry/delete transaction (queue/in-flight-key) job-id)
    (registry/add transaction (dead-letter-key) job-id)
    (jobs/update-props context job-id {:status "failed"
                                       :reason reason
                                       :failed-at (now-millis)})))
