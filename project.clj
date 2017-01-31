(defproject com.buckryan/farmhand "0.3.0-SNAPSHOT"
  :description "Simple and powerful background jobs"
  :url "https://github.com/b-ryan/farmhand"
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [redis.clients/jedis "2.9.0"]]
  :main ^:skip-aot farmhand.core
  :target-path "target/%s"
  :jvm-opts ["-Duser.timezone=GMT"]
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[log4j "1.2.17"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :1.6 {:dependencies [[org.clojure/clojure "1.6.0"]]}
             :1.7 {:dependencies [[org.clojure/clojure "1.7.0"]]}
             :1.8 {:dependencies [[org.clojure/clojure "1.8.0"]]}
             :1.9 {:dependencies [[org.clojure/clojure "1.9.0-alpha14"]]}})
