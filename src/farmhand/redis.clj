(ns farmhand.redis
  (:import (java.net URI)
           (redis.clients.jedis Jedis JedisPool JedisPoolConfig Protocol)))

(defn create-pool
  [{:keys [uri host port timeout-ms password database]
    :or {host "localhost"
         port Protocol/DEFAULT_PORT
         timeout-ms Protocol/DEFAULT_TIMEOUT
         database Protocol/DEFAULT_DATABASE}}]
  {:jedis-pool
   (if uri
     (JedisPool. (JedisPoolConfig.) (URI. ^String uri))
     (JedisPool. (JedisPoolConfig.)
                 ^String host
                 ^Integer port
                 ^Integer timeout-ms
                 ^String password
                 ^Integer database))})

(defn close-pool
  [{jedis :jedis-pool}]
  (.close ^JedisPool jedis))

(defn str-arr #^"[Ljava.lang.String;" [& args] (into-array args))
(defn seq->str-arr #^"[Ljava.lang.String;" [items] (into-array items))

(defmacro with-jedis*
  [[sym context] & body]
  `(if (:jedis ~context)
     (let [~sym ~context] ~@body)
     (with-open [cxn# (.getResource ^JedisPool (:jedis-pool ~context))]
       (let [~sym (assoc ~context :jedis cxn#)]
         ~@body))))

(defmacro with-transaction*
  [[sym context] & body]
  `(if (:transaction ~context)
     (let [~sym ~context] ~@body)
     (with-jedis* [ctx# ~context]
       (let [txn# (.multi ^Jedis (:jedis ctx#))
             ~sym (assoc ctx# :transaction txn#)
             ret# (do ~@body)]
         (.exec txn#)
         ret#))))

(def key-prefix "farmhand:")
(defn redis-key [& args] (apply str key-prefix args))
