(ns farmhand.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [farmhand.utils :as utils]))

(def ^:private all-env-vars (delay (System/getenv)))

(def classpath
  (delay (some-> (io/resource "farmhand/config.edn")
                 (slurp)
                 (edn/read-string))))

(def ^:private parsers {:int utils/parse-long})

(defn- getenv
  [[name & [parser]]]
  (when-let [val (get @all-env-vars name)]
    (let [parse (or (get parsers parser) parser identity)]
      (parse val))))

(defn merge*
  ([env-def file-path]
   (merge* env-def file-path nil))
  ([env-def file-path default]
   (or (get-in @classpath file-path)
       (getenv env-def)
       default)))

(defn redis
  [overrides]
  (merge (utils/filter-map-vals
           {:uri      (merge* ["FARMHAND_REDIS_URI"]           [:redis :uri])
            :host     (merge* ["FARMHAND_REDIS_HOST"]          [:redis :host])
            :port     (merge* ["FARMHAND_REDIS_PORT" :int]     [:redis :port])
            :password (merge* ["FARMHAND_REDIS_PASSWORD"]      [:redis :password])
            :database (merge* ["FARMHAND_REDIS_DATABASE" :int] [:redis :database])}
           #(not (nil? %)))
         overrides))

(def ^:private default-num-workers 2)

(defn num-workers
  [override]
  (or override
      (merge* ["FARMHAND_NUM_WORKERS" :int] [:num-workers] default-num-workers)))

(def ^:private default-queues [{:name "default"}])

(defn queues
  [override]
  (or override
      (merge* ["FARMHAND_QUEUES_EDN" edn/read-string] [:queues] default-queues)))

(def default-prefix "farmhand:")

(defn prefix
  [override]
  (or override
      (merge* ["FARMHAND_REDIS_PREFIX"] [:redis :prefix] default-prefix)))
