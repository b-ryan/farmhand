(ns farmhand.jobs
  (:require [clojure.edn :as edn]
            [clojure.string :refer [split]]
            [farmhand.redis :as r :refer [with-jedis with-transaction]]
            [farmhand.utils :as utils :refer [now-millis]])
  (:import (java.io FileNotFoundException)
           (java.util UUID)
           (redis.clients.jedis Jedis RedisPipeline)))

(def ttl-secs (* 60 60 24 60)) ;; 60 days
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

(defn save-new
  [context {job-id :job-id :as job}]
  ;; The typehint using RedisPipeline here is because using Transaction creates
  ;; an ambiguous typehint
  (with-transaction [{:keys [^RedisPipeline transaction]} context]
    (let [key (job-key context job-id)]
      (.hmset transaction key (prepare-to-save job))
      (.expire transaction key ttl-secs))))

(defn update-props
  [context job-id props]
  ;; The typehint using RedisPipeline here is because using Transaction creates
  ;; an ambiguous typehint
  (with-transaction [{:keys [^RedisPipeline transaction]} context]
    (.hmset transaction (job-key context job-id) (prepare-to-save props))))

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

(defn fetch-body
  [context job-id]
  (with-jedis [{:keys [^Jedis jedis]} context]
    (-> (into {} (.hgetAll jedis (job-key context job-id)))
        (utils/update-keys keyword)
        (utils/update-vals edn/read-string)
        (assoc-fn-var))))
