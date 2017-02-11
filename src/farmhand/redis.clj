(ns farmhand.redis
  (:import (java.net URI)
           (redis.clients.jedis Jedis JedisPool JedisPoolConfig Pipeline
                                Protocol RedisPipeline Transaction)))

(set! *warn-on-reflection* true)

(defn create-pool
  [{:keys [uri host port timeout-ms password database]
    :or {host "localhost"
         port Protocol/DEFAULT_PORT
         timeout-ms Protocol/DEFAULT_TIMEOUT
         database Protocol/DEFAULT_DATABASE}}]
  {:jedis
   (if uri
     (JedisPool. (JedisPoolConfig.) (URI. ^String uri))
     (JedisPool. (JedisPoolConfig.)
                 ^String host
                 ^Integer port
                 ^Integer timeout-ms
                 ^String password
                 ^Integer database))})

(defn close-pool
  [{jedis :jedis}]
  (.close ^JedisPool jedis))

(defn str-arr #^"[Ljava.lang.String;" [& args] (into-array args))
(defn seq->str-arr #^"[Ljava.lang.String;" [items] (into-array items))

(defmacro with-jedis
  [pool sym & body]
  (let [tagged-sym (vary-meta sym assoc :tag `Jedis)]
    `(with-open [~tagged-sym (.getResource ^JedisPool (:jedis ~pool))]
       ~@body)))

(defmacro with-transaction
  [pool sym & body]
  (let [tagged-sym (vary-meta sym assoc :tag `RedisPipeline)]
    `(with-jedis ~pool jedis#
       (let [~sym (.multi jedis#)]
         ~@body
         (.exec ~sym)))))

(def key-prefix "farmhand:")
(defn redis-key [& args] (apply str key-prefix args))
