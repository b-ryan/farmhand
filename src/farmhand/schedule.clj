(ns farmhand.schedule
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as q]
            [farmhand.redis :as r :refer [with-jedis with-transaction]]
            [farmhand.registry :as registry]
            [farmhand.utils :refer [now-millis safe-loop]]))

(defn registry-name [queue-name] (str "schedule:" queue-name))

(defn run-at*
  "Lower level function for scheduling a job. Puts the job on the schedule
  registry and updates the job's status to \"scheduled\". Returns the updated
  job."
  [context {job-id :job-id :as job} queue-name at]
  (with-transaction [context context]
    (registry/add context (registry-name queue-name) job-id :expire-at at)
    (jobs/update-props context job {:status "scheduled"})))

(defn run-at
  "Normalizes a job schedules it to run at some time in the future. Returns the
  updated job. See the docs in farmhand.core/run-at for more details."
  [context job at]
  (let [{queue-name :queue :as normalized} (jobs/normalize job)]
    (with-transaction [context context]
      (jobs/save context normalized)
      (run-at* context normalized queue-name at))))

(def ^:private multipliers {:milliseconds 1
                            :seconds 1000
                            :minutes (* 1000 60)
                            :hours (* 1000 60 60)
                            :days (* 1000 60 60 24)})

(defn from-now
  [n unit]
  {:pre [(get multipliers unit)]}
  (let [multiplier (get multipliers unit)]
    (+ (now-millis) (* n multiplier))))

(defn run-in
  "Schedules a job to run at some time relative to now. See the docs in
  farmhand.core/run-in for more details."
  [context job n unit]
  (run-at context job (from-now n unit)))

(defn registries
  [{:keys [queues] :as context}]
  (for [{queue-name :name} queues]
    {:name (registry-name queue-name)
     :cleanup-fn #(q/push %1 %2 queue-name)}))
