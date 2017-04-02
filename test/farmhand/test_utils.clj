(ns farmhand.test-utils
  (:require [farmhand.config :as cfg]
            [farmhand.redis :as r :refer [with-jedis]]))

(def test-prefix "farmhand-test:")
(def context {:jedis-pool (r/create-pool)
              :prefix test-prefix})

(defn cleanup-redis
  []
  (with-jedis [{:keys [jedis]} context]
    (try
      (.eval jedis "return redis.call('del', unpack(redis.call('keys', ARGV[1])))"
             0 (r/str-arr (str test-prefix "*")))
      (catch Exception e))))

(defn redis-test-fixture
  [f]
  (cleanup-redis)
  (f)
  (cleanup-redis))
