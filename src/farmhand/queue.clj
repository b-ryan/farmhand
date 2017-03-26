(ns farmhand.queue
  (:require [clojure.java.io :as io]
            [farmhand.jobs :as jobs]
            [farmhand.redis :as r :refer [with-jedis* with-transaction*]]
            [farmhand.registry :as registry]
            [farmhand.utils :refer [now-millis]])
  (:import (redis.clients.jedis Jedis Transaction)))

(defn all-queues-key ^String [] (r/redis-key "queues"))
(defn in-flight-key ^String [] (r/redis-key "inflight"))
(defn completed-key ^String [] (r/redis-key "completed"))
(defn queue-key ^String [queue-name] (r/redis-key "queue:" queue-name))

(defn push
  [context job-id queue-name]
  (with-transaction* [{:keys [^Transaction transaction] :as context} context]
    (.sadd transaction (all-queues-key) (r/str-arr queue-name))
    (.lpush transaction (queue-key queue-name) (r/str-arr job-id))
    (jobs/update-props context job-id {:status "queued"})))

(defn describe-queues
  "Returns a list of all queues and their current number of items."
  [context]
  (with-jedis* [{:keys [^Jedis jedis]} context]
    (doall (map (fn [^String queue-name]
                  {:name queue-name
                   :size (.llen jedis (queue-key queue-name))})
                (.smembers jedis (all-queues-key))))))

(defn purge
  "Deletes all items from a queue"
  [context queue-name]
  (with-jedis* [{:keys [^Jedis jedis]} context]
    (.del jedis (queue-key queue-name))))

(defn queue-order
  "Accepts a sequence of queue maps and returns a vector of queue names.

  Each queue map has keys:

  :name
  Name of the queue

  :priority
  (optional) This determines precedence of a queue. If queue A has a higher
  priority than queue B, then ALL jobs in queue A must be consumed before any
  in queue B will run.

  :weight
  (optional) A weight to give the queue. This is different than :priority. When
  queues A and B have the same priority, but queue A has weight 2 and queue B
  has weight 1, then queue A will be used twice as often as queue B."
  [queue-defs]
  {:post [(vector? %)]}
  (->> queue-defs
       ;; Take weight into consideration. If a queue has a a weight of N, we
       ;; repeat that queue N times in the resulting list.
       (mapcat #(repeat (get % :weight 1) %))
       ;; Shuffle to avoid one queue being treated with a higher priority when
       ;; there are other queues of the same priority.
       (shuffle)
       ;; Sort to take priority into account. Queues with higher priority will
       ;; jump to the top.
       (sort-by :priority #(compare %2 %1))
       (mapv :name)))

(def ^:private ^String dequeue-lua (slurp (io/resource "farmhand/dequeue.lua")))

(defn dequeue
  [context queue-names]
  {:pre [(vector? queue-names)]}
  (let [keys (mapv queue-key queue-names)
        now-str (str (now-millis))
        params (r/seq->str-arr (conj keys (in-flight-key) now-str))
        num-keys ^Integer (inc (count keys))]
    (with-jedis* [{:keys [^Jedis jedis]} context]
      (.eval jedis dequeue-lua num-keys params))))

(defn complete
  [context job-id & {:keys [result]}]
  (with-transaction* [{:keys [^Transaction transaction] :as context} context]
    (jobs/update-props context job-id {:status "complete"
                                       :result result
                                       :completed-at (now-millis)})
    (registry/delete transaction (in-flight-key) job-id)
    (registry/add transaction (completed-key) job-id)))
