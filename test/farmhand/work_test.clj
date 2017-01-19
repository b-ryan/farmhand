(ns farmhand.work-test
  (:require [clojure.test :refer :all]
            [farmhand.core :refer [enqueue]]
            [farmhand.dead-letters :as dead-letters]
            [farmhand.queue :as q]
            [farmhand.redis :refer [with-jedis]]
            [farmhand.test-utils :as tu]
            [farmhand.work :as work]))

(def last-call-args (atom nil))

(defn reset-last-call-args
  [f]
  (reset! last-call-args nil)
  (f))

(use-fixtures :each tu/redis-test-fixture reset-last-call-args)

(defn work-fn [& args] (reset! last-call-args args))

(deftest queue-and-run
  (let [job-id (enqueue {:fn-var #'work-fn :args [:a 1 3.4 "abc"]} tu/pool)]
    (testing "running the main work function will pull and execute the job"
      (work/run-once tu/pool [{:name "default"}])
      (is (= @last-call-args [:a 1 3.4 "abc"])))
    (testing "completed job has been added to the completed registry"
      (is (=
           (with-jedis tu/pool jedis
             (.zrange jedis (q/completed-key) 0 10))
           #{job-id})))))

(defn fail-fn [] (throw (Exception. "baz")))

(deftest dead-letters
  (let [job-id (enqueue {:fn-var #'fail-fn :args []} tu/pool)]
    (work/run-once tu/pool [{:name "default"}])
    (testing "failed job has been added to the dead letter registry"
      (is (=
           (with-jedis tu/pool jedis
             (.zrange jedis (dead-letters/dead-letter-key) 0 10))
           #{job-id})))))

(deftest requeuing
  (let [job-id (enqueue {:fn-var #'fail-fn :args []} tu/pool)]
    (work/run-once tu/pool [{:name "default"}])
    (dead-letters/requeue job-id tu/pool)))
