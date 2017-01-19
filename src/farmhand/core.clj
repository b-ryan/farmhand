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

(defn start-server
  ([] (start-server (config/load-config)))
  ([{queue-defs :queues redis-conf :redis pool :pool :as config}]
   (let [pool (or pool (redis/create-pool redis-conf))
         shutdown (atom false)
         work-futures (atom nil)
         mk-worker #(future (work/main-loop shutdown pool queue-defs))]
     (reset! work-futures (doall (repeatedly (:num-workers config) mk-worker)))
     {:pool pool
      :shutdown shutdown
      :work-futures work-futures})))

(defn stop-server
  [{:keys [shutdown work-futures]}]
  (do
    (reset! shutdown true)
    (doall (map #(deref %) @work-futures))
    (reset! shutdown false)))

(defn -main
  "I don't do a whole lot ... yet."
  [& _]
  (start-server))



(comment

  (do
    (def server* (start-server (assoc config/defaults :num-workers 2)))
    (defn slow-job [& args] (Thread/sleep 10000) :slow-result)
    (defn failing-job [& args] (throw (ex-info "foo" {:a :b}))))

  (enqueue {:fn-var #'slow-job :args ["i am slow"]} (:pool server*))
  (enqueue {:fn-var #'failing-job :args ["fail"]} (:pool server*))

  (stop-server server*)
  )
