(ns farmhand.registry
  (:require [clojure.core.async :as async]
            [farmhand.jobs :as jobs]
            [farmhand.redis :as r :refer [with-jedis with-transaction]]
            [farmhand.utils :refer [now-millis safe-loop-thread]])
  (:import (redis.clients.jedis Jedis RedisPipeline Transaction Tuple)))

(defn registry-key ^String [c reg-name] (r/redis-key c "registry:" reg-name))

(def ^:private default-ttl-ms (* 1000 60 60 24 60)) ;; 60 days
(defn- expiration [ttl-ms] (+ (now-millis) (or ttl-ms default-ttl-ms)))

(defn- delete*
  [context ^String job-id ^String reg-key]
  (with-transaction [{:keys [^Transaction transaction] :as context} context]
    (.zrem transaction reg-key (r/str-arr job-id))))

(def ^:private default-page-size 25)

(defn- page-raw
  [^Jedis jedis ^String reg-key {:keys [page size newest-first?]}]
  (let [page (or page 0)
        size (or size default-page-size)
        start (* page size)
        end (dec (+ start size))
        items (.zrangeWithScores jedis reg-key ^Long start ^Long end)
        comparator (if newest-first? #(compare %2 %1) #(compare %1 %2))]
    (->> items
         (map (fn [^Tuple tuple] {:expiration (long (.getScore tuple))
                                  :job (.getElement tuple)}))
         (sort-by :expiration comparator))))

(defn- last-page
  [^Jedis jedis ^String reg-key {:keys [page size] :as options}]
  (let [page (or page 0)
        size (or size default-page-size)]
    (-> (.zcard jedis reg-key)
        (/ size)
        (Math/ceil)
        (int)
        (dec))))

(defn add
  [context ^String job-id ^String registry & [{:keys [ttl-ms expire-at]}]]
  (let [exp (double (or expire-at (expiration ttl-ms)))]
    (with-transaction [{:keys [^RedisPipeline transaction]} context]
      (.zadd transaction (registry-key context registry) exp job-id))))

(defn delete
  [context job-id registry]
  (delete* context job-id (registry-key context registry)))

(defn page
  [context reg-name {:keys [page] :as options}]
  (with-jedis [{:keys [jedis] :as context} context]
    (let [fetcher #(update-in % [:job] (partial jobs/fetch context))
          reg-key (registry-key context reg-name)
          items (->> (page-raw jedis reg-key options)
                     (map fetcher))
          last-page (last-page jedis reg-key options)
          page (or page 0)]
      {:items items
       :prev-page (when (> page 0) (dec page))
       :next-page (when (< page last-page) (inc page))})))

(defn- fetch-ready-id
  "Fetches the next job-id that is ready to be popped off the registry."
  [^Jedis jedis ^String reg-key ^String now]
  (-> (.zrangeByScore jedis reg-key "-inf" now (int 0) (int 1))
      (first)))

(defn- remove-with-cleanup-fn
  [context ^String job-id ^String reg-key cleanup-fn]
  (let [job (jobs/fetch context job-id)]
    (with-transaction [context context]
      (delete* context job-id reg-key)
      (cleanup-fn context job))))

(defn cleanup
  [{:keys [registries] :as context}]
  (let [now-str (str (now-millis))]
    (with-jedis [{:keys [^Jedis jedis] :as context} context]
      (doseq [{reg-name :name cleanup-fn :cleanup-fn} registries
              :let [reg-key (registry-key context reg-name)]]
        (loop [num-removed 0]
          (.watch jedis (r/str-arr reg-key))
          (if-let [job-id (fetch-ready-id jedis reg-key now-str)]
            (do
              (remove-with-cleanup-fn context job-id reg-key cleanup-fn)
              (recur (inc num-removed)))
            (.unwatch jedis)))))))

(def ^:private loop-sleep-ms (* 1000 60))

(defn cleanup-thread
  [context stop-chan]
  (safe-loop-thread
    "registry"
    (async/alt!!
      stop-chan :exit-loop
      (async/timeout loop-sleep-ms) (cleanup context))))
