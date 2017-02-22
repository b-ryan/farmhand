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
  [transaction job expiration]
  `(with-redefs [registry/expiration (constantly ~expiration)]
     (jobs/save-new ~transaction ~job)
     (registry/add ~transaction (q/completed-key) (:job-id ~job))))

(defn save-jobs-fixture
  [f]
  (with-transaction tu/pool t
    (save t job1 1234)
    (save t job2 3456)
    (save t job3 5678))
  (f))

(use-fixtures :each tu/redis-test-fixture save-jobs-fixture)

(deftest sorts-items-by-oldest-first
  (is (= (registry/page (q/completed-key) tu/pool {})
         {:items [{:expiration 1234 :job job1}
                  {:expiration 3456 :job job2}
                  {:expiration 5678 :job job3}]
          :prev-page nil
          :next-page nil})))

(deftest sorts-items-by-newest-first
  (is (= (registry/page (q/completed-key) tu/pool {:newest-first? true})
         {:items [{:expiration 5678 :job job3}
                  {:expiration 3456 :job job2}
                  {:expiration 1234 :job job1}]
          :prev-page nil
          :next-page nil})))

(deftest pages-are-handled
  (is (= (registry/page (q/completed-key) tu/pool {:page 1 :size 1})
         {:items [{:expiration 3456 :job job2}]
          :prev-page 0
          :next-page 2})))

(deftest cleanup-removes-older-jobs
  (with-redefs [now-millis (constantly 4000)]
    (registry/cleanup tu/pool [(q/completed-key)]))
  (is (= (registry/page (q/completed-key) tu/pool {})
         {:items [{:expiration 5678 :job job3}]
          :prev-page nil
          :next-page nil})))
