(ns farmhand.jobs
  (:require [clojure.edn :as edn]
            [clojure.string :refer [split]]
            [farmhand.redis :as r :refer [with-jedis with-transaction]]
            [farmhand.utils :as utils :refer [now-millis]])
  (:import (java.io FileNotFoundException)
           (java.util UUID)
           (redis.clients.jedis Jedis RedisPipeline)))

(def default-queue "default")

(defn job-key ^String [c job-id] (r/redis-key c "job:" job-id))

(defn- fn-path
  [fn_]
  (format "%s/%s" (-> fn_ meta :ns ns-name) (-> fn_ meta :name)))

(defn normalize
  "Takes a new job definition and prepares it to be saved.

  Accepts:
    :queue
    :args
    :fn-var

  Adds:
    :job-id
    :created-at
    :fn-path
  "
  [{:keys [queue fn-var] :as job}]
  (-> job
      (assoc :queue (or queue default-queue)
             :job-id (str (UUID/randomUUID))
             :created-at (now-millis)
             :fn-path (fn-path fn-var))))

(defn- prepare-to-save
  [m]
  (-> m
      (dissoc :fn-var)
      (utils/update-keys name)
      (utils/update-vals pr-str)))

(defn save
  "Saves job data in Redis. Can save either the entire or a portion of its
  keys. For example, you can use this function to update just the status of a
  job using

    (save context job-id {:status \"something\"})

  You must either provide the job-id as the second argument or the keys being
  saved should contain the job-id. For example you could also save the status
  using

    (save context {:job-id job-id :status \"something\"})"
  ([context {job-id :job-id :as job}]
   (save context job-id job))
  ([context job-id properties]
   ;; The typehint using RedisPipeline here is because using Transaction creates
   ;; an ambiguous typehint
   ;; Transaction is used so it can be nested within other transactions
   (with-transaction [{:keys [^RedisPipeline transaction]} context]
     (.hmset transaction (job-key context job-id) (prepare-to-save properties)))))

(defn update-props
  "A small wrapper around 'save' that takes a job and some properties, saves
  the properties in Redis, and returns the properties merged into the job.

  For example:

    (update-props context {:job-id \"foo\"} {:status \"something\"})

  will save the jobs status as \"something\" and return

    {:job-id \"foo\" :status \"something\"}"
  [context {job-id :job-id :as job} props]
  (save context job-id props)
  (merge job props))

(defn- m-seq [m] (if (empty? m) nil m))

(defn assoc-fn-var
  [{:keys [fn-path] :as job}]
  ;; It is necessary to require the namespace or else there is no way to find
  ;; the var
  (try
    (some-> fn-path
            (split #"\/")
            (first)
            (symbol)
            (require))
    (catch FileNotFoundException e))
  (assoc job :fn-var (some-> fn-path (symbol) (resolve))))

(defn fetch
  [context job-id]
  (with-jedis [{:keys [^Jedis jedis]} context]
    (some-> (into {} (.hgetAll jedis (job-key context job-id)))
            (m-seq)
            (utils/update-keys keyword)
            (utils/update-vals edn/read-string)
            (assoc-fn-var))))

(defn delete
  [context job-id]
  (with-transaction [{:keys [^RedisPipeline transaction]} context]
    (.del transaction (job-key context job-id))))
