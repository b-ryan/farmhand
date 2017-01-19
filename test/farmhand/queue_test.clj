(ns farmhand.queue-test
  (:require [clojure.test :refer :all]
            [farmhand.core :refer [enqueue]]
            [farmhand.queue :as q]
            [farmhand.test-utils :as tu]))

(use-fixtures :each tu/redis-test-fixture)

(defn queued-fn [])

(deftest queue-dequeue
  (let [job-id (enqueue {:fn-var #'queued-fn :args []} tu/pool)]
    (is (q/dequeue tu/pool ["default"]) job-id)))

(deftest queue-ordering
  (let [job1 (enqueue {:fn-var #'queued-fn :args []} tu/pool)
        job2 (enqueue {:fn-var #'queued-fn :args []} tu/pool)
        job3 (enqueue {:fn-var #'queued-fn :args []} tu/pool)]
    (is (q/dequeue tu/pool ["default"]) job1)
    (is (q/dequeue tu/pool ["default"]) job2)
    (is (q/dequeue tu/pool ["default"]) job3)))
