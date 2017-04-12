(ns farmhand.work-test
  (:require [clojure.test :refer :all]
            [farmhand.core :refer [enqueue]]
            [farmhand.handler :refer [default-handler]]
            [farmhand.queue :as q]
            [farmhand.redis :refer [with-jedis]]
            [farmhand.test-utils :as tu]
            [farmhand.work :as work])
  (:import (redis.clients.jedis Jedis)))

(def last-call-args (atom nil))

(defn reset-last-call-args
  [f]
  (reset! last-call-args nil)
  (f))

(use-fixtures :each tu/redis-test-fixture reset-last-call-args)

(defn work-fn [& args] (reset! last-call-args args))

(deftest queue-and-run
  (let [job-id (enqueue tu/context {:fn-var #'work-fn :args [:a 1 3.4 "abc"]})]
    (testing "running the main work function will pull and execute the job"
      (work/run-once tu/context)
      (is (= @last-call-args [:a 1 3.4 "abc"])))
    (testing "completed job has been added to the completed registry"
      (is (=
           (with-jedis [{:keys [^Jedis jedis]} tu/context]
             (.zrange jedis tu/completed-key 0 10))
           #{job-id})))))

(defn fail-fn [] (throw (Exception. "baz")))

(deftest dead-letters
  (let [job-id (enqueue tu/context {:fn-var #'fail-fn :args []})]
    (work/run-once tu/context)
    (testing "failed job has been added to the dead letter registry"
      (is (= (with-jedis [{:keys [^Jedis jedis]} tu/context]
               (.zrange jedis tu/dead-key 0 10))
             #{job-id})))))

(deftest requeuing
  (let [job-id (enqueue tu/context {:fn-var #'fail-fn :args []})]
    (work/run-once tu/context)
    (q/requeue tu/context job-id)
    (testing "job is queued"
      (is (= (with-jedis [{:keys [^Jedis jedis]} tu/context]
               (.lrange jedis tu/queue-key 0 10))
             [job-id])))
    (testing "job is no longer on the dead letter registry"
      (is (= (with-jedis [{:keys [^Jedis jedis]} tu/context]
               (.zrange jedis tu/dead-key 0 10))
             #{})))))
