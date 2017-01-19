(ns farmhand.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [farmhand.redis :as r]
            [farmhand.utils :as utils]))

(def defaults {:redis {:host "localhost"}
               :num-workers 2
               :queues [{:name "default"}]})

(defn- read-from-classpath
  [config]
  (merge config
         (some-> (io/resource "farmhand/config.edn")
                 (slurp)
                 (edn/read-string))))

(def ^:private env-vars
  [["FARMHAND_REDIS_URI"   {:path [:redis :uri]}]
   ["FARMHAND_NUM_WORKERS" {:parse utils/parse-long :path [:num-workers]}]
   ["FARMHAND_QUEUES_EDN"  {:parse edn/read-string :path [:queues]}]])

(defn- getenv [e] (System/getenv e)) ;; Function exists so we can mock it in tests

(defn- read-from-environment
  [config]
  (reduce (fn [config [env-var {:keys [parse path] :or {parse identity}}]]
            (if-let [val (getenv env-var)]
              (assoc-in config path (parse val))
              config))
          config
          env-vars))

(defn load-config
  "High-level function which returns a fully-baked configuration.

  The steps this function takes are:

  - Reads values from the classpath file and environment variables
  - Merges those values with the default configuration
  - Initializes a Redis pool"
  []
  (-> defaults
      read-from-classpath
      read-from-environment))
