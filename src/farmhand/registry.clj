(ns farmhand.registry
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [farmhand.jobs :as jobs]
            [farmhand.redis :as r :refer [with-jedis with-transaction]]
            [farmhand.utils :refer [now-millis safe-loop]])
  (:import (redis.clients.jedis Jedis RedisPipeline Transaction Tuple)))

(defn all-registries-key ^String [c] (r/redis-key c "registries"))

(def ^:private default-ttl-ms (* 1000 60 60 24 60)) ;; 60 days
(defn- expiration [ttl-ms] (+ (now-millis) (or ttl-ms default-ttl-ms)))

(defn add
  [context ^String key ^String job-id & {:keys [ttl-ms]}]
  (with-transaction [{:keys [^RedisPipeline transaction]} context]
    (.sadd transaction (all-registries-key context) (r/str-arr key))
    (.zadd transaction key (double (expiration ttl-ms)) job-id)))

(defn delete
  [context ^String key ^String job-id]
  (with-transaction [{:keys [^RedisPipeline transaction]} context]
    (.zrem transaction key (r/str-arr job-id))))

(defn- num-items
  [context ^String key]
  (with-jedis [{:keys [^Jedis jedis]} context]
    (.zcard jedis key)))

(defn- all-registries
  [context]
  (with-jedis [{:keys [^Jedis jedis]} context]
    (.smembers jedis (all-registries-key context))))

(def ^:private default-size 25)

(defn- page-raw
  [^Jedis jedis ^String key {:keys [page size newest-first?]}]
  (let [page (or page 0)
        size (or size default-size)
        start (* page size)
        end (dec (+ start size))
        items (.zrangeWithScores jedis key ^Long start ^Long end)
        comparator (if newest-first? #(compare %2 %1) #(compare %1 %2))]
    (->> items
         (map (fn [^Tuple tuple] {:expiration (long (.getScore tuple))
                                  :job (.getElement tuple)}))
         (sort-by :expiration comparator))))

(defn- last-page
  [^Jedis jedis ^String key {:keys [page size] :as options}]
  (let [page (or page 0)
        size (or size default-size)]
    (-> (.zcard jedis key)
        (/ size)
        (Math/ceil)
        (int)
        (dec))))

(defn page
  [context key {:keys [page] :as options}]
  (with-jedis [{:keys [jedis] :as context} context]
    (let [fetcher #(update-in % [:job] (partial jobs/fetch-body context))
          items (->> (page-raw jedis key options)
                     (map fetcher))
          last-page (last-page jedis key options)
          page (or page 0)]
      {:items items
       :prev-page (when (> page 0) (dec page))
       :next-page (when (< page last-page) (inc page))})))

(defn- fetch-ready-id
  "Fetches the next job-id that is ready to be popped off the registry."
  [^Jedis jedis ^String reg-key ^String now]
  (-> (.zrangeByScore jedis reg-key "-inf" now (int 0) (int 1))
      (first)))

(defn- remove-from-registry
  [context ^String reg-key ^String job-id]
  (with-transaction [{:keys [^Transaction transaction] :as context} context]
    ;; TODO do something different with the job depending on the
    ;; registry type
    (jobs/delete context job-id)
    (.zrem transaction reg-key (r/str-arr job-id))))

(defn cleanup
  [context]
  (let [now-str (str (now-millis))]
    (with-jedis [{:keys [^Jedis jedis]} context]
      (doseq [reg-key (all-registries context)]
        (loop [num-removed 0]
          (.watch jedis (r/str-arr reg-key))
          (if-let [job-id (fetch-ready-id jedis reg-key now-str)]
            (do
              (remove-from-registry context reg-key job-id)
              (recur (inc num-removed)))
            (do
              (.unwatch jedis)
              (when (> num-removed 0)
                (log/debugf "Removed %d items from %s" num-removed reg-key)))))))))

(def ^:private loop-sleep-ms (* 1000 60))

(defn cleanup-thread
  [context stop-chan]
  (async/thread
    (log/info "in registry cleanup thread")
    (safe-loop
      (async/alt!!
        stop-chan :exit-loop
        (async/timeout loop-sleep-ms) (cleanup context)))
    (log/info "exiting registry cleanup thread")))
