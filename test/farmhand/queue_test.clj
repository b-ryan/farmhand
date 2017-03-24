(ns farmhand.queue-test
  (:require [clojure.test :refer :all]
            [farmhand.core :refer [enqueue]]
            [farmhand.queue :as q]
            [farmhand.test-utils :as tu]))

(use-fixtures :each tu/redis-test-fixture)

(defn queued-fn [])

(deftest queue-dequeue
  (let [job-id (enqueue tu/pool {:fn-var #'queued-fn :args []})]
    (is (q/dequeue tu/pool ["default"]) job-id)))

(deftest queue-ordering
  (let [job1 (enqueue tu/pool {:fn-var #'queued-fn :args []})
        job2 (enqueue tu/pool {:fn-var #'queued-fn :args []})
        job3 (enqueue tu/pool {:fn-var #'queued-fn :args []})]
    (is (q/dequeue tu/pool ["default"]) job1)
    (is (q/dequeue tu/pool ["default"]) job2)
    (is (q/dequeue tu/pool ["default"]) job3)))

(deftest queue-describing
  (dotimes [_ 7] (enqueue tu/pool {:fn-var #'queued-fn :args []}))
  (dotimes [_ 9] (enqueue tu/pool {:fn-var #'queued-fn :args [] :queue "foo"}))
  (is (= (set (q/describe-queues tu/pool)) #{{:name "default" :size 7}
                                             {:name "foo" :size 9}})))

(deftest queue-purging
  (dotimes [_ 7] (enqueue tu/pool {:fn-var #'queued-fn :args []}))
  (q/purge tu/pool "default")
  (is (= (q/describe-queues tu/pool) [{:name "default" :size 0}])))
