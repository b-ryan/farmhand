(ns farmhand.schedule
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [farmhand.jobs :as jobs]
            [farmhand.queue :as q]
            [farmhand.redis :as r :refer [with-jedis with-transaction]]
            [farmhand.registry :as registry]
            [farmhand.utils :refer [from-now]]))

(def registry "scheduled")

(defn run-at
  "Normalizes a job schedules it to run at some time in the future. Returns the
  updated job. See the docs in farmhand.core/run-at for more details."
  [context job at]
  (let [{queue-name :queue job-id :job-id :as job} (jobs/normalize job)]
    (with-transaction [context context]
      (registry/add context job-id registry {:expire-at at})
      (jobs/save context (assoc job :status "scheduled")))))

(defn run-in
  "Schedules a job to run at some time relative to now. See the docs in
  farmhand.core/run-in for more details."
  [context job n unit]
  (run-at context job (from-now n unit)))

(defn schedule
  "Function for handling jobs that have expired from the schedule registry."
  [context {:keys [job-id queue]}]
  (q/push context job-id queue))
