(ns farmhand.work-test
  (:require [clojure.test :refer :all]
            [farmhand.core :refer [enqueue]]
            [farmhand.handler :refer [default-handler]]
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
  (let [job-id (enqueue tu/pool {:fn-var #'work-fn :args [:a 1 3.4 "abc"]})]
    (testing "running the main work function will pull and execute the job"
      (work/run-once tu/pool [{:name "default"}] default-handler)
      (is (= @last-call-args [:a 1 3.4 "abc"])))
    (testing "completed job has been added to the completed registry"
      (is (=
           (with-jedis [{:keys [jedis]} tu/pool]
             (.zrange jedis (q/completed-key tu/pool) 0 10))
           #{job-id})))))

(defn fail-fn [] (throw (Exception. "baz")))

(deftest dead-letters
  (let [job-id (enqueue tu/pool {:fn-var #'fail-fn :args []})]
    (work/run-once tu/pool [{:name "default"}] default-handler)
    (testing "failed job has been added to the dead letter registry"
      (is (=
           (with-jedis [{:keys [jedis]} tu/pool]
             (.zrange jedis (q/dead-letter-key tu/pool) 0 10))
           #{job-id})))))

(deftest requeuing
  (let [job-id (enqueue tu/pool {:fn-var #'fail-fn :args []})]
    (work/run-once tu/pool [{:name "default"}] default-handler)
    (q/requeue tu/pool job-id)))
