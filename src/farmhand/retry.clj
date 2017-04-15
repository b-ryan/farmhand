(ns farmhand.retry
  (:require [clojure.tools.logging :as log]
            [farmhand.jobs :as jobs]
            [farmhand.redis :refer [with-transaction]]
            [farmhand.queue :as queue]
            [farmhand.registry :as registry]
            [farmhand.schedule :as schedule]
            [farmhand.utils :refer [now-millis rethrow-if-fatal]]))

(defmulti update-retry
  "Multimethod which takes a job containing a retry map and returns a new retry
  map. A return value of nil indicates the job should not be retried."
  (comp :strategy :retry))

(defmethod update-retry nil [_] nil)

;; 0   1   2   3   4    5    6    7    =  8 attempts
;; 1 + 2 + 4 + 8 + 16 + 32 + 64 + 128  =  ~10 days
(def ^:private backoff-defaults
  {:num-attempts 0
   :max-attempts 8
   :coefficient 1
   :delay-unit :hours})

(defmethod update-retry "backoff"
  ;; A retry strategy that uses an exponential backoff with a maximum number
  ;; of retries. By default, will cause the first retry to occur in roughly 1
  ;; hour, the next in 2 hours, the next in 4 hours, etc. until the maximum
  ;; number of attempts has occurred.
  [{:keys [retry] :as job}]
  (let [retry (merge backoff-defaults retry)
        {:keys [num-attempts max-attempts coefficient]} retry
        delay-time (* coefficient (Math/pow 2 num-attempts))]
    (when (< (inc num-attempts) max-attempts)
      (assoc retry
             :num-attempts (inc num-attempts)
             :delay-time (int delay-time)))))
