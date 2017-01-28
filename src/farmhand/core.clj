(ns farmhand.core
  (:require [farmhand.config :as config]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as queue]
            [farmhand.redis :as redis :refer [with-jedis]]
            [farmhand.work :as work])
  (:import (java.util.concurrent Executors TimeUnit))
  (:gen-class))

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

(defn enqueue
  "Pushes a job onto the queue. Returns the job's ID.

  The second argument is a Farmhand pool. If a pool is not given, the value in
  the pool* atom will be used."
  ([job]
   (enqueue job @pool*))
  ([job pool]
   (with-jedis pool jedis
     (let [job (jobs/normalize job)
           transaction (.multi jedis)]
       (jobs/save-new transaction job)
       (queue/push transaction job)
       (.exec transaction)
       (:job-id job)))))

(defn start-server
  ([] (start-server {}))
  ([config-overrides]
   (let [config (merge (config/load-config) config-overrides)
         pool (or (:pool config) (redis/create-pool (:redis config)))
         shutdown (atom false)

         thread-pool (Executors/newFixedThreadPool (:num-workers config))
         run-worker #(work/main-loop shutdown pool (:queues config))
         _ (doall (repeatedly (:num-workers config)
                              #(.submit thread-pool ^Runnable run-worker)))

         server {:pool pool
                 :shutdown shutdown
                 :thread-pool thread-pool}]
     (dosync
       (reset! pool* pool)
       (reset! server* server))
     server)))

(defn stop-server
  "Stops a running Farmhand server. If no server is given, this function will
  stop the server in the server* atom.

  By default this waits up to 2 minutes for the running jobs to complete. This
  value can be overriden with the :timeout-ms option."
  ([] (stop-server @server*))
  ([{:keys [pool shutdown thread-pool]} & {:keys [timeout-ms]
                                           :or {timeout-ms (* 1000 60 2)}}]
   (do
     (reset! shutdown true)
     (.shutdown thread-pool)
     (.awaitTermination thread-pool timeout-ms TimeUnit/MILLISECONDS)
     (redis/close-pool pool))))

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
