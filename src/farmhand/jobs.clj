(ns farmhand.jobs
  (:require [clojure.edn :as edn]
            [clojure.string :refer [split]]
            [farmhand.redis :as r :refer [with-jedis]]
            [farmhand.utils :as utils :refer [now-millis]])
  (:import (java.io FileNotFoundException)
           (java.util UUID)
           (redis.clients.jedis Jedis RedisPipeline)))

(def ttl-secs (* 60 60 24 60)) ;; 60 days
(def default-queue "default")

(defn job-key ^String [job-id] (r/redis-key "job:" job-id))

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
  [^RedisPipeline pipeline {job-id :job-id :as job}]
  (let [key (job-key job-id)]
    (.hmset pipeline key (prepare-to-save job))
    (.expire pipeline key ttl-secs)))

(defn update-props
  [^RedisPipeline pipeline job-id props]
  (.hmset pipeline (job-key job-id) (prepare-to-save props)))

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

(defn fetch-body*
  [job-id ^Jedis jedis]
  (-> (into {} (.hgetAll jedis (job-key job-id)))
      (utils/update-keys keyword)
      (utils/update-vals edn/read-string)
      (assoc-fn-var)))

(defn fetch-body
  [job-id pool]
  (with-jedis pool jedis
    (fetch-body* job-id jedis)))
