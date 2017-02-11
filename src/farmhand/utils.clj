(ns farmhand.utils
  (:require [clojure.tools.logging :as log]))

(defn update-map
  [m f]
  (into {} (for [[k v] m] (f k v))))

(defn update-keys
  "Applies a function to all keys of a map and returns a new map.

  Example:

  (update-keys {1 :foo 2 :bar} inc)
  => {2 :foo 3 :bar}"
  [m f]
  (into {} (for [[k v] m] [(f k) v])))

(defn update-vals
  "Applies a function to all values of a map and returns a new map.

  Example:

  (update-vals {:foo 1 :bar 2} inc)
  => {:foo 2 :bar 3}"
  [m f]
  (into {} (for [[k v] m] [k (f v)])))

(defn filter-map-keys
  "Like filter, but for maps. Accepts a map and a function and returns a new
  map. Only keys where (f k) is true will be in the returned map."
  [m f]
  (into {} (filter (fn [[k _]] (f k)) m)))

(defn filter-map-vals
  "Like filter, but for maps. Accepts a map and a function and returns a new
  map. Only values where (f v) is true will be in the returned map."
  [m f]
  (into {} (filter (fn [[_ v]] (f v)) m)))

(defn catchable?
  "Predicate that indicates whether a given instance of Throwable is catchable.

  A Throwable is considered catchable if is an instance of either Exception or
  AssertionError"
  [e]
  (or (instance? Exception e)
      (instance? AssertionError e)))

(def fatal? "Opposite of catchable?" (complement catchable?))

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
