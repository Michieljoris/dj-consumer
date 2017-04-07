(ns dj-consumer.util
  (:require
   [clojure.walk :as walk]
   [clj-time.core :as t]
   [clj-time.format :as tf]
   [yaml.core :as yaml]

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

(defn parse-natural-number
  "Reads and returns an integer from a string, or the param itself if
  it's already a natural number. Returns nil if not a natural
  number (includes 0)"
  [s]
  (cond
    (and (string? s) (re-find #"^\d+$" s)) (read-string s)
    (and (number? s) (>= s 0))  s
    :else nil))


(defn split-on-hyphen
  "Splits a string on hyphens."
  [s]
  (str/split s #"-"))

(defn split-on-underscore
  "Splits a string on undescores."
  [s]
  (str/split s #"_"))

(defn split-camel-case
  "Splits a camel case string into tokens. Consecutive captial lets,
  except for the last one, become a single token."
  [s]
  (-> s
      (.replaceAll "([A-Z]+)([A-Z][a-z])" "$1-$2")
      (.replaceAll "([a-z\\d])([A-Z])" "$1-$2")
      (split-on-hyphen)))

(defn split-camel-case-sticky
  "Splits a camel case string, keeping consecutive capital characters
  attached to the following token."
  [s]
  (split-on-hyphen (.replaceAll s "([a-z\\d])([A-Z])" "$1-$2")))

(defn camel->hyphen
  [s]
  (str/join "-" (split-camel-case s)))

(defn camel-sticky->hyphen
  [s]
  (str/join "-" (split-camel-case-sticky s)))

(defn camel->keyword
  ([s] (camel->keyword nil s))
  ([ns s]
   (if (string? s)
     (let [lower-hyphen (-> s camel->hyphen str/lower (str/strip-prefix ":"))
           ns (if ns (-> ns camel->hyphen str/lower (str/strip-prefix ":")))
           lower-hyphen (if ns (str ns "/" lower-hyphen) lower-hyphen)]
       (keyword lower-hyphen)))))

(def sql-formatter (tf/formatter "yyyy-MM-dd HH:mm:ss"))

(defn to-sql-time-string [t]
  (tf/unparse sql-formatter t))

(defn remove-!ruby-annotations [s]
  (str/replace s #"!ruby/[^\s]*" ""))

(defn extract-rails-struct-name[s]
  (second (re-find  #"--- *!ruby\/struct:([^\s]*)" s)))

(defn extract-rails-obj-name[s]
  (second (re-find  #"object: *!ruby\/object:([^\s]*)" s)))

(defn parse-ruby-yaml [s]
  (let [s (or s "")
        struct-name (extract-rails-struct-name s)
        object-name (extract-rails-obj-name s)
        data (yaml/parse-string
              (remove-!ruby-annotations s))
        method-name (:method_name data)]
    {:name (or (camel->keyword struct-name)
               (if (and object-name method-name) (camel->keyword object-name method-name))
               :unknown-job-name)
     :payload data}))
