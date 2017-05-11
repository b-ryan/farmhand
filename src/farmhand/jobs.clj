(ns farmhand.jobs
  (:require [clojure.string :refer [split]]
            [farmhand.config :as config]
            [farmhand.redis :as r :refer [with-jedis with-transaction]]
            [farmhand.utils :as utils :refer [now-millis]])
  (:import (java.io FileNotFoundException)
           (java.util UUID)
           (redis.clients.jedis Jedis RedisPipeline)))

(defn job-key ^String [c job-id] (r/redis-key c "job:" job-id))

(defn- fn-var->path
  [fn_]
  (format "%s/%s" (-> fn_ meta :ns ns-name) (-> fn_ meta :name)))

(defn- assoc-fn-var
  "Given a job map, finds and assocs the :fn-var on the map based on the job's
  :fn-path. If the function cannot be found, :fn-var will be set to nil."
  [{:keys [fn-path] :as job}]
  ;; Below the 'require' is needed because 'resolve' doesn't work otherwise
  (try
    (some-> fn-path
            (split #"\/")
            (first)
            (symbol)
            (require))
    (catch FileNotFoundException e))
  (assoc job :fn-var (some-> fn-path symbol resolve)))

(defn- encode-for-redis
  [job]
  (-> job
      (dissoc :fn-var)
      (utils/map-xform {:args pr-str
                        :result pr-str
                        :retry pr-str})
      (utils/update-keys name)
      (utils/update-vals str)))

(defn- decode-from-redis
  [job]
  (-> job
      (utils/update-keys keyword)
      (utils/map-xform {:created-at utils/parse-long
                        :stopped-at utils/parse-long
                        :args read-string
                        :result utils/read-string-safe
                        :retry utils/read-string-safe})
      (assoc-fn-var)))

(defn throw-if-invalid
  [{:keys [fn-var] :as job}]
  (when-not fn-var
    (throw (ex-info (str "This job does not have an :fn-var set. :fn-var must be "
                         "a Clojure var pointing to some function.")
                    {:job job})))
  (try
    (decode-from-redis (encode-for-redis job))
    (catch Exception e
      (throw (ex-info (str "This job cannot be queued because there is an "
                           "issue serializing it for Redis. This probably means "
                           "there is some data in :args that we cannot read "
                           "using read-string, which we must use since this data "
                           "is being stored in Redis. The solution is to try to use "
                           "simple data types, like integers and strings, in your "
                           "args.")
                      {:job job} e)))))

(defn normalize
  "Takes a job definition and prepares it to be saved."
  [{:keys [queue job-id created-at fn-path fn-var] :as job}]
  (assoc job
         :queue (or queue config/default-queue)
         :job-id (or job-id (str (UUID/randomUUID)))
         :created-at (or created-at (now-millis))
         :fn-path (or fn-path (fn-var->path fn-var))))

(defn save
  [context {job-id :job-id :as job}]
  ;; The typehint using RedisPipeline here is because using Transaction creates
  ;; an ambiguous typehint
  ;; Transaction is used so it can be nested within other transactions
  (with-transaction [{:keys [^RedisPipeline transaction]} context]
    (.hmset transaction (job-key context job-id) (encode-for-redis job)))
  job)

(defn fetch
  "Retrieves the body of a job from Redis."
  [context job-id]
  (with-jedis [{:keys [^Jedis jedis]} context]
    (some-> (into {} (.hgetAll jedis (job-key context job-id)))
            (utils/map-seq)
            (decode-from-redis)
            (assoc-fn-var))))

(defn delete
  [context {:keys [job-id]}]
  (with-transaction [{:keys [^RedisPipeline transaction]} context]
    (.del transaction (job-key context job-id))))
