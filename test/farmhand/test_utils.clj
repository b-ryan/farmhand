(ns farmhand.test-utils
  (:require [farmhand.config :as cfg]
            [farmhand.core :refer [assoc-registries]]
            [farmhand.handler :refer [default-handler]]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as q]
            [farmhand.redis :as r :refer [with-jedis]]
            [farmhand.registry :refer [registry-key]]
            [farmhand.schedule :as schedule]))

(def test-prefix "farmhand-test:")

(def context (-> {:jedis-pool (r/create-pool)
                  :prefix test-prefix
                  :queues [{:name "default"}]
                  :handler default-handler}
                 assoc-registries))

(def ^String queue-key (q/queue-key context "default"))
(def ^String completed-key (registry-key context q/completed-registry))
(def ^String dead-key (registry-key context q/dead-letter-registry))
(def ^String in-flight-key (registry-key context q/in-flight-registry))
(def ^String schedule-key (registry-key context (schedule/registry-name "default")))

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
