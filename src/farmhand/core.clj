(ns farmhand.core
  (:require [clojure.core.async :as async]
            [farmhand.config :as config]
            [farmhand.handler :as handler]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as queue]
            [farmhand.redis :as redis :refer [with-jedis with-transaction]]
            [farmhand.registry :as registry]
            [farmhand.scheduled :as scheduled]
            [farmhand.work :as work])
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
   (let [{:keys [job-id queue] :as job} (jobs/normalize job)]
     (with-transaction pool transaction
       (jobs/save-new transaction job)
       (queue/push transaction job-id queue))
     job-id)))

(defn start-server
  [& [{:keys [num-workers queues redis pool handler]}]]
  (let [queues (config/queues queues)
        pool (or pool (redis/create-pool (config/redis redis)))
        handler (or handler handler/default-handler)
        stop-chan (async/chan)
        threads (concat
                  (for [_ (range (config/num-workers num-workers))]
                    (work/work-thread pool stop-chan queues handler))
                  [(registry/cleanup-thread pool stop-chan)
                   (scheduled/schedule-thread pool stop-chan)])
        server {:pool pool
                :stop-chan stop-chan
                :threads (doall threads)}]
    (dosync
      (reset! pool* pool)
      (reset! server* server))
    server))

(defn stop-server
  "Stops a running Farmhand server. If no server is given, this function will
  stop the server in the server* atom."
  ([] (stop-server @server*))
  ([{:keys [pool stop-chan threads]}]
   (async/close! stop-chan)
   (async/<!! (async/merge threads))
   (redis/close-pool pool)))

(defn -main
  [& _]
  (start-server))


(comment

  (do
    (start-server {:handler (handler/wrap-debug handler/default-handler)})
    (defn slow-job [& args] (Thread/sleep 10000) :slow-result)
    (defn failing-job [& args] (throw (ex-info "foo" {:a :b}))))

  (enqueue {:fn-var #'slow-job :args ["i am slow"]} @pool*)
  (enqueue {:fn-var #'failing-job :args ["fail"]} @pool*)

  (stop-server)
  )
