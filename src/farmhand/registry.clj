(ns farmhand.registry
  (:require [farmhand.jobs :as jobs]
            [farmhand.redis :as redis :refer [with-jedis]]
            [farmhand.utils :refer [now-millis]])
  (:import (redis.clients.jedis RedisPipeline Tuple)))

(set! *warn-on-reflection* true)

(def ^:private ttl-ms (* 1000 60 60 24 30)) ;; 30 days

(defn expiration [] (+ (now-millis) ttl-ms))

(defn add
  [^RedisPipeline pipeline ^String key ^String job-id]
  (.zadd pipeline key (double (expiration)) job-id))

(defn delete
  [^RedisPipeline pipeline ^String key ^String job-id]
  (.zrem pipeline key (redis/str-arr job-id)))

(defn- page-raw
  [^String key pool {:keys [page size oldest-first?] :or {page 0 size 25}}]
  (let [start (* page size)
        end (dec (+ start size))
        items (with-jedis pool jedis
                (.zrangeWithScores jedis key ^Long start ^Long end))
        comparator (if oldest-first? #(compare %1 %2) #(compare %2 %1))]
    (->> items
         (map (fn [^Tuple tuple] [(long (.getScore tuple))
                                  (.getElement tuple)]))
         (sort-by first comparator))))

(defn page
  [key pool options]
  (let [results (page-raw key pool options)
        fetch #(update-in % [1] jobs/fetch-body pool)]
    {:items (map fetch results)}))
