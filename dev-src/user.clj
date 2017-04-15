(ns user)


(defn wrap-debug
  "Utility function provided for convenience. Logs the request and response."
  [handler]
  (fn debug [request]
    (log/debugf "received request %s" request)
    (let [response (handler request)]
      (log/debugf "received response %s" response)
      response)))


(comment

  (do
    (start-server {:handler (handler/wrap-debug handler/default-handler)})
    (defn slow-job [& args] (Thread/sleep 10000) :slow-result)
    (defn failing-job [& args] (throw (ex-info "foo" {:a :b}))))

  (enqueue @context* {:fn-var #'slow-job :args [(clj-time.core/now)]})
  (enqueue @context* {:fn-var #'failing-job :args ["fail"]})
  (enqueue @context* {:fn-var #'failing-job :args ["fail"]
                      :retry {:strategy "backoff"
                              :delay-unit :minutes
                              :max-attempts 2}})

  (schedule/run-in @context* {:fn-var #'slow-job :args ["i am slow"]} 1 :minutes)

  (stop-server))
