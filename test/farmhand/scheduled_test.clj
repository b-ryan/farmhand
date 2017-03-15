(ns farmhand.scheduled-test
  (:require [clojure.test :refer :all]
            [farmhand.queue :as queue]
            [farmhand.scheduled :as scheduled]
            [farmhand.test-utils :as tu]
            [farmhand.utils :as utils]))

(def fake-now 1000)
(def earlier-than-now 900)
(def later-than-now 1200)

(defn redef-now
  [f]
  (with-redefs [utils/now-millis (constantly fake-now)]
    (f)))

(use-fixtures :each tu/redis-test-fixture redef-now)

(defn work-fn [])

(deftest run-at
  (let [job-id (scheduled/run-at tu/pool {:fn-var #'work-fn} earlier-than-now)]
    (is (seq job-id))
    (is (nil? (queue/dequeue tu/pool ["default"])))
    (scheduled/pull-and-enqueue tu/pool)
    (is (= (queue/dequeue tu/pool ["default"]) job-id))))

(deftest run-at-job-in-future
  (let [job-id (scheduled/run-at tu/pool {:fn-var #'work-fn} later-than-now)]
    (is (seq job-id))
    (is (nil? (queue/dequeue tu/pool ["default"])))
    (scheduled/pull-and-enqueue tu/pool)
    (is (nil? (queue/dequeue tu/pool ["default"])))))

(deftest run-in
  (let [job-id (scheduled/run-in tu/pool {:fn-var #'work-fn} 10 :seconds)]
    (is (seq job-id))
    (is (nil? (queue/dequeue tu/pool ["default"])))
    (with-redefs [utils/now-millis (constantly (+ fake-now (* 1000 20)))]
      (scheduled/pull-and-enqueue tu/pool))
    (is (= (queue/dequeue tu/pool ["default"]) job-id))))

(deftest run-in-not-ready-yet
  (let [job-id (scheduled/run-in tu/pool {:fn-var #'work-fn} 10 :seconds)]
    (is (seq job-id))
    (is (nil? (queue/dequeue tu/pool ["default"])))
    (scheduled/pull-and-enqueue tu/pool)
    (is (nil? (queue/dequeue tu/pool ["default"])))))
