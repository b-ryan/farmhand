(ns farmhand.retry-test
  (:require [clojure.test :refer :all]
            [farmhand.core :as fc]
            [farmhand.handler :refer [default-handler]]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as q]
            [farmhand.redis :refer [with-jedis]]
            [farmhand.schedule :as s]
            [farmhand.work :as work]
            [farmhand.retry :as retry]
            [farmhand.test-utils :as tu]))

(use-fixtures :each tu/redis-test-fixture)

(defn fail [] (throw (ex-info "foo" {:a 2})))

(deftest failures-handled-with-retry
  (let [job-id (fc/enqueue tu/pool {:fn-var #'fail :retry {:strategy "backoff"}})]
    (work/run-once tu/pool [{:name "default"}] default-handler)
    (with-jedis [{:keys [jedis]} tu/pool]
      (testing "job was scheduled to be run again"
        (is (= (.zrange jedis (s/schedule-key tu/pool "default") 0 10) #{job-id})))
      (testing "job has not been added to either dead letters / completed"
        (is (= (.zrange jedis (q/completed-key tu/pool) 0 10) #{}))
        (is (= (.zrange jedis (q/dead-letter-key tu/pool) 0 10) #{})))
      (testing "job as removed from in progress"
        (is (= (.zrange jedis (q/in-flight-key tu/pool) 0 10) #{}))
        (is (= (:status (jobs/fetch-body tu/pool job-id)) "scheduled"))))))

(deftest max-attempts-reached
  (let [job-id (fc/enqueue tu/pool {:fn-var #'fail :retry {:strategy "backoff" :max-attempts 1}})]
    (work/run-once tu/pool [{:name "default"}] default-handler)
    (with-jedis [{:keys [jedis]} tu/pool]
      (testing "job was scheduled to be run again"
        (is (= (.zrange jedis (s/schedule-key tu/pool "default") 0 10) #{})))
      (testing "job has been added to dead letters"
        (is (= (.zrange jedis (q/completed-key tu/pool) 0 10) #{}))
        (is (= (.zrange jedis (q/dead-letter-key tu/pool) 0 10) #{job-id}))))))
