(ns farmhand.registry-test
  (:require [clojure.test :refer :all]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as q]
            [farmhand.redis :refer [with-transaction]]
            [farmhand.registry :as registry]
            [farmhand.test-utils :as tu]))

(use-fixtures :each tu/redis-test-fixture)

(defn work-fn [])

(deftest add-and-get
  (let [job {:job-id "foo" :fn-path "abc/def"}]
    (with-redefs [registry/expiration (constantly 1234)]
      (with-transaction tu/pool t
        (jobs/save-new t job)
        (registry/add t (q/completed-key) "foo")))
    (is (= (registry/page (q/completed-key) tu/pool {})
           {:items [[1234 {:job-id "foo" :fn-path "abc/def" :fn-var nil}]]}))))
