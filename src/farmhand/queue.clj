(ns farmhand.queue
  (:require [clojure.java.io :as io]
            [farmhand.jobs :as jobs]
            [farmhand.redis :as r :refer [with-jedis with-transaction]]
            [farmhand.registry :as registry]
            [farmhand.utils :refer [now-millis]])
  (:import (redis.clients.jedis Jedis Transaction)))

(defn all-queues-key ^String [c] (r/redis-key c "queues"))
(defn queue-key ^String [c queue-name] (r/redis-key c "queue:" queue-name))
(def in-flight-registry "inflight")
(def completed-registry "completed")
(def dead-letter-registry "dead")

(defn push
  [context job-id queue-name]
  (with-transaction [{:keys [^Transaction transaction] :as context} context]
    (.sadd transaction (all-queues-key context) (r/str-arr queue-name))
    (.lpush transaction (queue-key context queue-name) (r/str-arr job-id))
    (jobs/save context job-id {:status "queued"})))

(defn describe-queues
  "Returns a list of all queues and their current number of items."
  [context]
  (with-jedis [{:keys [^Jedis jedis]} context]
    (doall (map (fn [^String queue-name]
                  {:name queue-name
                   :size (.llen jedis (queue-key context queue-name))})
                (.smembers jedis (all-queues-key context))))))

(defn purge
  "Deletes all items from a queue"
  [context queue-name]
  (with-jedis [{:keys [^Jedis jedis]} context]
    (.del jedis (queue-key context queue-name))))

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
  (let [keys (mapv #(queue-key context %) queue-names)
        now-str (str (now-millis))
        in-flight-key (registry/registry-key context in-flight-registry)
        params (r/seq->str-arr (conj keys in-flight-key now-str))
        num-keys ^Integer (inc (count keys))]
    (with-jedis [{:keys [^Jedis jedis] :as context} context]
      (let [job-id (.eval jedis dequeue-lua num-keys params)]
        ;; TODO make the status update part of the script???
        ;; may require saving statuses outside of the job
        (jobs/save context job-id {:status "processing"})
        job-id))))

(defn requeue
  "Puts job-id back onto its queue after it has failed."
  [context job-id]
  (with-jedis [{:keys [jedis] :as context} context]
    (let [{:keys [queue]} (jobs/fetch context job-id)]
      (with-transaction [context context]
        (registry/delete context dead-letter-registry job-id)
        (push context job-id queue)))))
