(ns dj-consumer.util
  (:require

   [clojure.core.async :as async]
   [clojure.walk :as walk]
   [clj-time.core :as time]
   [clj-time.format :as time-format]
   [clj-time.local :as time-local]
   [clj-time.coerce :as time-coerce]

   [yaml.core :as yaml]

   ;; String manipulation
   [cuerdas.core :as str]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]
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

(def sql-formatter (time-format/formatter "yyyy-MM-dd HH:mm:ss"))

(defn to-sql-time-string [t]
  (time-format/unparse sql-formatter t))

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
        data  (yaml/parse-string
               (remove-!ruby-annotations s))
        method-name (:method_name data)]
    {:name (or (camel->keyword struct-name)
               (if (and object-name method-name) (camel->keyword object-name method-name))
               :unknown-job-name)
     :payload data}
    ))

(defmacro runtime
  "Evaluates expr and returns a map with the time it took in ms
  under :runtime and the result of the expression under :result "
  [expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     {:runtime (/ (double (- (. System (nanoTime)) start#)) 1000000.0)
      :result ret#}))

;; (runtime (do (Thread/sleep 1000) :foo))
;; => {:result :foo, :runtime 1000.432838}

(defn parse-ex-info [e]
  {:msg (.getMessage e)
   :context (ex-data e)
   ;; :stacktrace
   ;; (.getStackTrace e)
   })

;;Unused, using channel version, see worker.clj
(defn timeout-using-future
  "This blocks till either f has completed or timeout expires,
  whichever comes first. If timeout occurs first, thread is not
  actually stopped and will still run its course. However it's
  possible to check for (Thread/interrupted) in f and end early. Will
  throw if timeout occers, or if f throws exception "
  [f ms]
  (try
    (let [fut (future (f))
          future-result (deref fut ms :timeout)]
      (when (= future-result :timeout)
        (future-cancel fut)
        (throw (ex-info (str "Function " (str f) " has timed out.") {:timeout? true}))
        ;; future-result
        ))
    (catch Exception e
      (let [{:keys [msg context]} (parse-ex-info e)]
        (cond
          (:timeout? context) (throw e)
          (.getCause e) (throw (.getCause e))
          :else (throw e))))))

(defn table-name
  "Table names are singular hyphenated and keywords. This fn returns
  the actual table name by looking it up in db-config.schema in env or
  otherwise just adds an 's'. If table is already a string it is
  returned as is.Returns a string or nil."
  [{:keys [db-config] :as env} table]
  (cond
    (keyword? table) (-> (or (get-in db-config [:schema table :table-name])
                             (if (:pluralize-table-names? db-config)
                               (str (name table) "s")
                               table))
                         name
                         hyphen->underscore)
    (string? table) table))

(defn sql-time
    "Return some-time in utc, with explicit local timezone offset added,
, so for instance you would get 2017-04-12T11:15:21.000+02:00 in
  Amsterdam at 13:15:21.000 local time"
    [some-time]
  (time/from-time-zone some-time (time/default-time-zone)))

(defn now
    "Return now with milliseconds set to 0"
    []
  (let [now (time/now)]
    (time/minus now (time/millis (time/milli now)))))

(def custom-formatter
  ;; (time-format/formatter "yyyy-MM-dd HH-mm-ss Z")
  (time-format/formatters :rfc822))

(defn exception-str [e]
  (if e
    (str (.toString e) "\nStacktrace:\n" (with-out-str (pprint (.getStackTrace e))))
    "exception=nil?"))

(defn time->str [some-time]
  (time-format/unparse custom-formatter some-time))



;;Unused, using channel version, see worker.clj
(defn timeout-using-future
  "This blocks till either f has completed or timeout expires,
  whichever comes first. If timeout occurs first, thread is not
  actually stopped and will still run its course. However it's
  possible to check for (Thread/interrupted) in f and end early. Will
  throw if timeout occers, or if f throws exception "
  [f ms]
  (try
    (let [fut (future (f))
          future-result (deref fut ms :timeout)]
      (when (= future-result :timeout)
        (future-cancel fut)
        (throw (ex-info (str "Function " (str f) " has timed out.") {:timeout? true}))
        ;; future-result
        ))
    (catch Exception e
      (let [{:keys [msg context]} (parse-ex-info e)]
        (cond
          (:timeout? context) (throw e)
          (.getCause e) (throw (.getCause e))
          :else (throw e))))))


(defn get-local-tz-offset []
  (let [now-in-ms (time-coerce/to-long (time/now))
        offset (/ (.getOffset (time/default-time-zone) now-in-ms) 1000)
        hours (int (/ offset 3600))
        minutes (int (/ (rem offset 3600) 60))]
    {:hours hours :minutes minutes}))

;; (now)
;; (time/now)
;; (time-local/to-local-date-time (time/now))
;; (now)
;; (time/to-time-zone (time/now) (time/default-time-zone))

;; (time-format/show-formatters)


;; (do
;;   (defn time-hogger [job]
;;     ;; (throw (ex-info "job error" {}))
;;     (loop [n 1]
;;       (/ n (+ 1 1.0))
;;       (if (and (not @(:stop? job)) (< n 100000000))
;;         (recur (inc n)))
;;       )
;;     (if @(:stop? job)
;;       (info "stopped")
;;       (info "done"))
;;     :time-hogger
;;     ;; (dotimes [n 10000000000]
;;     ;;   (/ n (+ n 1.0))
;;     ;;   )
;;     ;; (info (Thread/interrupted))
;;     ;; (info (Thread/interrupted))
;;     ;; (if (Thread/interrupted)
;;     ;;   (pprint "interrupted!!")
;;     ;;   (pprint "Hello"))
;;     )
;;   (time (time-hogger {:stop? (atom false)})))

;; (do
;;   (defn throwing-timeout2
;;     "This blocks till either f has completed or timeout expires,
;;   whichever comes first. If timeout occurs first, thread is not
;;   actually stopped and will still run its course. However it's
;;   possible to deref stop? prop of job in f and end early. Will
;;   throw if timeout occers, or if f throws exception "
;;     [f job ms]
;;     (let [timeout-channel (timeout ms)
;;           job-channel (thread (try
;;                                 (f)
;;                                 (catch Exception e
;;                                   e)))]
;;       (alt!!
;;         job-channel ([v _] (if (instance? Exception v)
;;                              (throw v)
;;                              v))
;;         timeout-channel (do
;;                           (reset! (:stop? job) true)
;;                           (throw (ex-info (str "Job "(:name job) " has timed out.") {:timeout? true}))))))

;;   ;; (def job {:name "some-job" :stop? (atom false)})
;;   ;; (throwing-timeout2 #(time-hogger job) job 2000)
;;   )


;; (def e (Exception.))
;; (instance? Exception e)
;; (try
;;   ;; (throw (Exception. "foo"))
;;   (throw (ex-info "bla" {:foo :bar}))
;;   (catch Exception e
;;     (let [{{:keys [foo]} :context} (parse-ex-info e)]
;;       (pprint foo)
;;       (pprint (parse-ex-info e)))))

;; (do
;;   (defn invoke []
;;     (try
;;       (pprint "foobar")
;;       (throw (Exception. "in invoke"))
;;       (catch Exception e
;;         (info "in invoke fn:" (.toString e))
;;         (info "throwing exception again")
;;         (throw e))
;;       (finally
;;         (pprint "finally")
;;         ;; (throw (Exception. "finally exception"))
;;         )))

;;   (invoke))

;; (do
;;   (def t (async/thread
;;            (try
;;              (invoke)
;;              (catch Exception e
;;                (info "in async/thread" (.toString e))
;;                :value-from-tread-exception)

;;              )))
;;   (pprint (async/<!! t)))
