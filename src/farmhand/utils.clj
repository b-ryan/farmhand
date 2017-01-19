(ns farmhand.utils
  (:require [clojure.tools.logging :as log]))

(defn update-keys
  [m f]
  (into {} (for [[k v] m] [(f k) v])))

(defn update-vals
  [m f]
  (into {} (for [[k v] m] [k (f v)])))

(defn fatal?
  [e]
  (not (or (instance? Exception e)
           (instance? AssertionError e))))

(defmacro safe-while
  [test-expr & body]
  `(while ~test-expr
     (try
       ~@body
       (catch Throwable e#
         (when (fatal? e#)
           (log/error e# "exiting safe-while due to fatal error")
           (throw e#))
         (log/error e# "unexpected exception, going to pause execution")
         (Thread/sleep (* 1000 2))))))

(defn parse-long
  [x]
  (when x (Long/parseLong x)))

(defn now-millis
  []
  (System/currentTimeMillis))
