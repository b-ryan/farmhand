(ns farmhand.jobs
  (:require [clojure.string :refer [split]]
            [farmhand.redis :as r :refer [with-jedis with-transaction]]
            [farmhand.utils :as utils :refer [now-millis]])
  (:import (java.io FileNotFoundException)
           (java.util UUID)
           (redis.clients.jedis Jedis RedisPipeline)))

(def default-queue "default")

(defn job-key ^String [c job-id] (r/redis-key c "job:" job-id))

(defn- fn-var->path
  [fn_]
  (format "%s/%s" (-> fn_ meta :ns ns-name) (-> fn_ meta :name)))

(defn normalize
  "Takes a job definition and prepares it to be saved."
  [{:keys [queue job-id created-at fn-path fn-var] :as job}]
  (assoc job
         :queue (or queue default-queue)
         :job-id (or job-id (str (UUID/randomUUID)))
         :created-at (or created-at (now-millis))
         :fn-path (or fn-path (fn-var->path fn-var))))

(defn- prepare-to-save
  [m]
  (-> m
      (dissoc :fn-var)
      (utils/update-keys name)
      (utils/update-vals pr-str)))

(defn save
  [context {job-id :job-id :as job}]
  ;; The typehint using RedisPipeline here is because using Transaction creates
  ;; an ambiguous typehint
  ;; Transaction is used so it can be nested within other transactions
  (with-transaction [{:keys [^RedisPipeline transaction]} context]
    (.hmset transaction (job-key context job-id) (prepare-to-save job)))
  job)

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
            (utils/update-vals read-string)
            (assoc-fn-var))))

(defn delete
  [context job-id]
  (with-transaction [{:keys [^RedisPipeline transaction]} context]
    (.del transaction (job-key context job-id))))
