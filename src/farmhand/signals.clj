(ns farmhand.signals
  (:import (sun.misc Signal SignalHandler)))

(defn- handler
  [f]
  (reify SignalHandler
    (handle [self signal] (f (.getName signal)))))

(defn register
  ([f] (register "USR2" f))
  ([signal f]
   (Signal/handle (Signal. signal) (handler f))))
