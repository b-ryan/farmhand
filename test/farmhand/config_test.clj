(ns farmhand.config-test
  (:require [clojure.test :refer :all]
            [farmhand.config :as cfg]))

(deftest test-redis-config
  (is (= (with-redefs [cfg/all-env-vars (atom {"FARMHAND_REDIS_HOST" "abc"
                                               "FARMHAND_REDIS_PORT" "123"
                                               "FARMHAND_REDIS_URI" "foo"
                                               "FARMHAND_REDIS_PASSWORD" "pw"})
                       cfg/classpath (atom {:redis {:password "betterpw"}})]
           (cfg/redis {:uri "bar"}))
         {:host "abc" :port 123 :uri "bar" :password "betterpw"})))

(deftest test-num-workers-config
  (is (= (with-redefs [cfg/all-env-vars (atom {"FARMHAND_NUM_WORKERS" "4"})]
           (cfg/num-workers nil))
         4))
  (is (= (with-redefs [cfg/all-env-vars (atom {"FARMHAND_NUM_WORKERS" "4"})
                       cfg/classpath (atom {:num-workers 8})]
           (cfg/num-workers nil))
         8))
  (is (= (with-redefs [cfg/all-env-vars (atom {"FARMHAND_NUM_WORKERS" "4"})
                       cfg/classpath (atom {:num-workers 8})]
           (cfg/num-workers 12))
         12)))

(deftest test-queues-config
  (is (= (with-redefs [cfg/all-env-vars (atom {"FARMHAND_QUEUES_EDN" "[{:name \"foo\"}]"})]
           (cfg/queues nil))
         [{:name "foo"}]))
  (is (= (with-redefs [cfg/all-env-vars (atom {"FARMHAND_QUEUES_EDN" "[{:name \"foo\"}]"})
                       cfg/classpath (atom {:queues [{:name "bar"}]})]
           (cfg/queues nil))
         [{:name "bar"}]))
  (is (= (with-redefs [cfg/all-env-vars (atom {"FARMHAND_QUEUES_EDN" "[{:name \"foo\"}]"})
                       cfg/classpath (atom {:queues [{:name "bar"}]})]
           (cfg/queues [{:name "baz"}]))
         [{:name "baz"}])))
