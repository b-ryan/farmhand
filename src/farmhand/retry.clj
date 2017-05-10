(ns farmhand.retry)

(defmulti update-job
  "Multimethod which takes a job containing a retry map and returns a new job
  with the retry key updated. When the :retry is set to nil, the job will not
  be retried."
  (comp :strategy :retry))

(defmethod update-job nil [job] (assoc job :retry nil))

;; 0   1   2   3   4    5    6    7    =  8 attempts
;; 1 + 2 + 4 + 8 + 16 + 32 + 64 + 128  =  ~10 days
(def ^:private backoff-defaults
  {:num-attempts 0
   :max-attempts 8
   :coefficient 1
   :delay-unit :hours})

(defmethod update-job "backoff"
  ;; A retry strategy that uses an exponential backoff with a maximum number
  ;; of retries. By default, will cause the first retry to occur in roughly 1
  ;; hour, the next in 2 hours, the next in 4 hours, etc. until the maximum
  ;; number of attempts has occurred.
  [{:keys [retry] :as job}]
  (let [retry (merge backoff-defaults retry)
        {:keys [num-attempts max-attempts coefficient]} retry
        delay-time (* coefficient (Math/pow 2 num-attempts))]
    ;; TODO add randomness
    (assoc job :retry
           (when (< (inc num-attempts) max-attempts)
             (assoc retry
                    :num-attempts (inc num-attempts)
                    :delay-time (int delay-time))))))
