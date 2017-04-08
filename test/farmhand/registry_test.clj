(ns farmhand.registry-test
  (:require [clojure.test :refer :all]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as q]
            [farmhand.redis :refer [with-transaction]]
            [farmhand.registry :as registry]
            [farmhand.utils :refer [now-millis]]
            [farmhand.test-utils :as tu]))

(defn work-fn [])

(def job1 {:job-id "foo" :fn-path "abc/def" :fn-var nil})
(def job2 {:job-id "bar" :fn-path "abc/def" :fn-var nil})
(def job3 {:job-id "baz" :fn-path "abc/def" :fn-var nil})

(defmacro save
  [context job expiration]
  `(with-redefs [registry/expiration (constantly ~expiration)]
     (jobs/save-new ~context ~job)
     (registry/add ~context (q/completed-key ~context) (:job-id ~job))))

(defn save-jobs-fixture
  [f]
  (with-transaction [context tu/context]
    (save context job1 1234)
    (save context job2 3456)
    (save context job3 5678))
  (f))

(use-fixtures :each tu/redis-test-fixture save-jobs-fixture)

(deftest sorts-items-by-oldest-first
  (is (= (registry/page tu/context (q/completed-key tu/context) {})
         {:items [{:expiration 1234 :job job1}
                  {:expiration 3456 :job job2}
                  {:expiration 5678 :job job3}]
          :prev-page nil
          :next-page nil})))

(deftest sorts-items-by-newest-first
  (is (= (registry/page tu/context (q/completed-key tu/context) {:newest-first? true})
         {:items [{:expiration 5678 :job job3}
                  {:expiration 3456 :job job2}
                  {:expiration 1234 :job job1}]
          :prev-page nil
          :next-page nil})))

(deftest pages-are-handled
  (is (= (registry/page tu/context (q/completed-key tu/context) {:page 1 :size 1})
         {:items [{:expiration 3456 :job job2}]
          :prev-page 0
          :next-page 2})))

(deftest cleanup-removes-older-jobs
  (with-redefs [now-millis (constantly 4000)]
    (registry/cleanup tu/context))
  (is (= (registry/page tu/context (q/completed-key tu/context) {})
         {:items [{:expiration 5678 :job job3}]
          :prev-page nil
          :next-page nil}))
  (is (nil? (jobs/fetch-body tu/context (:job-id job1))))
  (is (nil? (jobs/fetch-body tu/context (:job-id job2))))
  (is (= (jobs/fetch-body tu/context (:job-id job3)) job3)))
