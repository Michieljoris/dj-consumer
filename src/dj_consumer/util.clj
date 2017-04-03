(ns dj-consumer.util
  (:require
   [clojure.walk :as walk]

   ;; String manipulation
   [cuerdas.core :as str]
   ))

(defn parse-ex-info [e]
  {:msg (.getMessage e)
   :context (ex-data e)
   :stacktrace (.getStackTrace e)})

(defn includes?
  "Returns true if collection c includes element e"
  [c e]
  (some #(= % e) c))

;; https://github.com/jeremyheiler/wharf/blob/master/src/wharf/core.clj
(defn transform-keys
  "Recursively transforms all map keys in coll with t."
  [t coll]
  (let [f (fn [[k v]] [(t k) v])]
    (walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) coll)))

(defn transform-values
  "Recursively transforms all map values in coll with t."
  [t coll]
  (let [f (fn [[k v]] [k (t v)])]
    (walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) coll)))

;; (deftest foo (is (= 1 2)))

(defn underscore->hyphen
  [s]
  (str/replace s #"_" "-"))

(defn hyphen->underscore
  [s]
  (str/replace s #"-" "_"))

(defn keyword->underscored-string [k]
  (if k (hyphen->underscore (name k))))
