(ns farmhand.retry-test
  (:require [clojure.test :refer :all]
            [farmhand.core :as fc]
            [farmhand.handler :refer [default-handler]]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as q]
            [farmhand.redis :refer [with-jedis]]
            [farmhand.work :as work]
            [farmhand.retry :as retry]
            [farmhand.test-utils :as tu])
  (:import (redis.clients.jedis Jedis)))

(use-fixtures :each tu/redis-test-fixture)

(defn fail [] (throw (ex-info "foo" {:a 2})))

(deftest failures-handled-with-retry
  (let [job-id (fc/enqueue tu/context {:fn-var #'fail :retry {:strategy :backoff}})]
    (work/run-once tu/context)
    (with-jedis [{:keys [^Jedis jedis]} tu/context]
      (testing "job was scheduled to be run again"
        (is (= (.zrange jedis tu/schedule-key 0 10) #{job-id})))
      (testing "job has not been added to either dead letters / completed"
        (is (= (.zrange jedis tu/completed-key 0 10) #{}))
        (is (= (.zrange jedis tu/dead-key 0 10) #{})))
      (testing "job as removed from in progress"
        (is (= (.zrange jedis tu/in-flight-key 0 10) #{}))
        (is (= (:status (jobs/fetch tu/context job-id)) "scheduled"))))))

(deftest max-attempts-reached
  (let [job-id (fc/enqueue tu/context {:fn-var #'fail :retry {:strategy :backoff :max-attempts 1}})]
    (work/run-once tu/context)
    (with-jedis [{:keys [^Jedis jedis]} tu/context]
      (testing "job was scheduled to be run again"
        (is (= (.zrange jedis tu/schedule-key 0 10) #{})))
      (testing "job has been added to dead letters"
        (is (= (.zrange jedis tu/completed-key 0 10) #{}))
        (is (= (.zrange jedis tu/dead-key 0 10) #{job-id}))))))
