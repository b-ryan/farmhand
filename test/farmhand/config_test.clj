(ns farmhand.config-test
  (:require [clojure.test :refer :all]
            [farmhand.config :as cfg]))

(deftest test-loading-config
  (is (= (with-redefs [cfg/getenv {"FARMHAND_QUEUES_EDN" "[{:name \"foobar\" :weight 3}]"}]
           (cfg/load-config))
         {:redis {}
          :num-workers 4 ;; Set in the dev-resources/farmhand/config.edn file
          :queues [{:name "foobar" :weight 3}]})))

