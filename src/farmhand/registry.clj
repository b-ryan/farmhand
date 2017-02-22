(ns farmhand.registry
  (:require [clojure.tools.logging :as log]
            [farmhand.jobs :as jobs]
            [farmhand.redis :as redis :refer [with-jedis]]
            [farmhand.utils :refer [now-millis safe-while]])
  (:import (redis.clients.jedis Jedis RedisPipeline Tuple)))

(set! *warn-on-reflection* true)

(def ^:private ttl-ms (* 1000 60 60 24 30)) ;; 30 days

(defn expiration [] (+ (now-millis) ttl-ms))

(defn add
  [^RedisPipeline pipeline ^String key ^String job-id]
  (.zadd pipeline key (double (expiration)) job-id))

(defn delete
  [^RedisPipeline pipeline ^String key ^String job-id]
  (.zrem pipeline key (redis/str-arr job-id)))

(defn- num-items
  [^Jedis jedis ^String key]
  (.zcard jedis key))

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
  [key pool {:keys [page] :as options}]
  (with-jedis pool jedis
    (let [fetch-body #(update-in % [:job] jobs/fetch-body* jedis)
          items (->> (page-raw jedis key options)
                     (map fetch-body))
          last-page (last-page jedis key options)
          page (or page 0)]
      {:items items
       :prev-page (when (> page 0) (dec page))
       :next-page (when (< page last-page) (inc page))})))

(defn cleanup
  [pool keys]
  (let [now (now-millis)]
    (with-jedis pool jedis
      (doseq [^String key keys]
        (.zremrangeByScore jedis key (double 0) (double now))))))

(defn cleanup-loop
  [shutdown pool keys]
  (log/info "in registry cleanup loop" keys)
  (safe-while (not @shutdown)
    (cleanup pool keys))
  (log/info "exiting registry cleanup loop"))
