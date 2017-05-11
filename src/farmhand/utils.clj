(ns farmhand.utils
  (:require [clojure.tools.logging :as log]))

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

(defn map-xform
  "Given a map 'm' and a map 'key-fns' of keys to functions, applies each
  function in 'key-fns' to 'm' where the keys in 'm' and 'key-fns' match.

  Example:

  (map-xform {:a 1 :b 2 :c 3} {:a inc :b str})
  => {:a 2 :b \"2\" :c 3}"
  [m key-fns]
  (reduce (fn [new-m [k f]]
            (if (contains? new-m k)
              (update-in new-m [k] f)
              new-m))
          m
          key-fns))

(defn filter-map-vals
  "Like filter, but for maps. Accepts a map and a function and returns a new
  map. Only values where (f v) is true will be in the returned map."
  [m f]
  (into {} (filter (fn [[_ v]] (f v)) m)))

(defn catchable?
  "Predicate that indicates whether a given instance of Throwable is catchable.

  This function is mainly aimed at top-level loops. For example it is used in
  the safe-loop macro to determine whether to continue executing the body of
  the loop when a Throwable is caught."
  [e]
  (cond
    (instance? AssertionError e) true
    (instance? Exception e) true
    :else false))

(def fatal? "Opposite of catchable?" (complement catchable?))
(defn rethrow-if-fatal [e] (when (fatal? e) (throw e)))

(defmacro safe-loop
  [& body]
  `(loop [status# nil]
     (when-not (= status# :exit-loop)
       (recur
         (try
           ~@body
           (catch Throwable e#
             (when (fatal? e#)
               (log/error e# "Rethrowing fatal error in safe-loop")
               (throw e#))
             (log/error e# "unexpected exception, going to pause execution")
             (Thread/sleep (* 1000 2))))))))

(defmacro safe-loop-thread
  [desc & body]
  `(async/thread
     (log/info (str "in " ~desc " thread"))
     (safe-loop
       ~@body)
     (log/info (str "exiting " ~desc " thread"))))

(defn parse-long
  [x]
  (when x (Long/parseLong x)))

(defn now-millis
  []
  (System/currentTimeMillis))

(def ^:private multipliers
  {:milliseconds 1
   :seconds     1000
   :minutes  (* 1000 60)        ;; ooo a pyramid
   :hours   (* 1000 60 60)
   :days   (* 1000 60 60 24)})

(defn from-now
  [n unit]
  {:pre [(get multipliers unit)]}
  (let [multiplier (get multipliers unit)]
    (+ (now-millis) (* n multiplier))))

(defn map-seq "Like seq, but for maps." [m] (if (empty? m) nil m))

(defn read-string-safe
  "Like read-string, but if there is an exception reading the string, returns
  nil."
  [s]
  (try (read-string s) (catch Exception e nil)))
