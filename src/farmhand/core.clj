(ns farmhand.core
  (:require [farmhand.config :as config]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as queue]
            [farmhand.redis :as redis :refer [with-jedis]]
            [farmhand.work :as work])
  (:gen-class))

(defn enqueue
  "Saves job to Redis and pushes it onto a queue."
  [job pool]
  (with-jedis pool jedis
    (let [job (jobs/normalize job)
          transaction (.multi jedis)]
      (jobs/save-new transaction job)
      (queue/push transaction job)
      (.exec transaction)
      (:job-id job))))

(defonce
  ^{:doc "An atom that contains the Jedis pool of the most recent invocation of
         start-server.

         For many applications, this atom will do the trick when you want to
         spin up a single server in your application and have easy access to
         the pool."}
  pool*
  (atom nil))

(defonce
  ^{:doc "An atom that contains the most recent server that was spun up. You
         can use this to easily stop a server without needing to store the
         server instance yourself."}
  server*
  (atom nil))

(defn start-server
  ([] (start-server {}))
  ([config-overrides]
   (let [config (merge (config/load-config) config-overrides)
         pool (or (:pool config) (redis/create-pool (:redis config)))
         shutdown (atom false)
         work-futures (atom nil)
         worker-fn #(future (work/main-loop shutdown pool (:queues config)))
         workers (doall (repeatedly (:num-workers config) worker-fn))
         _ (reset! work-futures workers)
         server {:pool pool
                 :shutdown shutdown
                 :work-futures work-futures}
         ]
     (dosync
       (reset! pool* pool)
       (reset! server* server))
     server)))

(defn stop-server
  [{:keys [pool shutdown work-futures]}]
  (do
    (reset! shutdown true)
    (doall (map #(deref %) @work-futures))
    (redis/close-pool pool)))

(defn -main
  [& _]
  (start-server))



(comment

  (do
    (start-server {:num-workers 4})
    (defn slow-job [& args] (Thread/sleep 10000) :slow-result)
    (defn failing-job [& args] (throw (ex-info "foo" {:a :b}))))

  (enqueue {:fn-var #'slow-job :args ["i am slow"]} @pool*)
  (enqueue {:fn-var #'failing-job :args ["fail"]} @pool*)

  (stop-server @server*)
  )
