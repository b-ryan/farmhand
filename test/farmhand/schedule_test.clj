(ns farmhand.schedule-test
  (:require [clojure.test :refer :all]
            [farmhand.core :refer [run-at run-in]]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as queue]
            [farmhand.registry :as registry]
            [farmhand.schedule :as schedule]
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

(deftest run-at-successful
  (let [job-id (run-at tu/context {:fn-var #'work-fn} earlier-than-now)]
    (is (seq job-id))
    (is (nil? (queue/dequeue tu/context ["default"])))
    (is (= (:status (jobs/fetch tu/context job-id)) "scheduled"))
    (registry/cleanup tu/context)
    (is (= (queue/dequeue tu/context ["default"]) job-id))))

(deftest run-at-job-in-future
  (let [job-id (run-at tu/context {:fn-var #'work-fn} later-than-now)]
    (is (seq job-id))
    (is (nil? (queue/dequeue tu/context ["default"])))
    (registry/cleanup tu/context)
    (is (nil? (queue/dequeue tu/context ["default"])))))

(deftest run-in-successful
  (let [job-id (run-in tu/context {:fn-var #'work-fn} 10 :seconds)]
    (is (seq job-id))
    (is (nil? (queue/dequeue tu/context ["default"])))
    (with-redefs [utils/now-millis (constantly (+ fake-now (* 1000 20)))]
      (registry/cleanup tu/context))
    (is (= (queue/dequeue tu/context ["default"]) job-id))))

(deftest run-in-not-ready-yet
  (let [job-id (run-in tu/context {:fn-var #'work-fn} 10 :seconds)]
    (is (seq job-id))
    (is (nil? (queue/dequeue tu/context ["default"])))
    (registry/cleanup tu/context)
    (is (nil? (queue/dequeue tu/context ["default"])))))
