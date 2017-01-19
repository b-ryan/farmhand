(ns farmhand.test-utils
  (:require [farmhand.config :as cfg]
            [farmhand.redis :as r :refer [with-jedis]]))

(def pool (r/create-pool cfg/defaults))
(def test-prefix "farmhand-test:")

(defn cleanup-redis
  []
  (with-jedis pool jedis
    (try
      (.eval jedis "return redis.call('del', unpack(redis.call('keys', ARGV[1])))"
             0 (r/str-arr (str test-prefix "*")))
      (catch Exception e))))

(defn redis-test-fixture
  [f]
  (with-redefs [r/key-prefix test-prefix]
    (cleanup-redis)
    (f)
    (cleanup-redis)))
