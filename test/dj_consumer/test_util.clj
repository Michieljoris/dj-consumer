(ns dj-consumer.test-util
  (:require
   [clojure.core.async :as async]
   [clojure.test :refer [use-fixtures is deftest testing]]
   [dj-consumer.database.queries :as mysql]
   [dj-consumer.database.connection :as db-conn]
   [dj-consumer.database.core :as db]
   [dj-consumer.worker :as worker]
   [yaml.core :as yaml]

   ;; String manipulation
   [cuerdas.core :as str]
   [digicheck.common.util :as u :refer [includes? transform-keys underscore->hyphen hyphen->underscore]]

   [clojure.set :as set]
   [clj-time.core :as time]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log trace debug info warn error fatal report color-str
                logf tracef debugf infof warnf errorf fatalf reportf
                spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   ;; https://github.com/xsc/jansi-clj
   ;; (println "ERROR:" (underline "This" " is " "a message."))
   [jansi-clj.core :refer :all]))

(def defaults
  (let [test-db-name "dj_consumer_test"
        job-table :job-table
        db-config {:user "root"
                   :password ""
                   :url "//localhost:3306/"
                   :schema {:job-table {:priority :int
                                        :id :int
                                        :attempts :int
                                        :handler :long-text
                                        :last-error :long-text
                                        :run-at :date-time
                                        :locked-at :date-time
                                        :failed-at :date-time
                                        :locked-by :text
                                        :queue :text}}
                   :db-name test-db-name}]
    {:test-db-name test-db-name
     :job-table job-table
     :db-config db-config
     :mysql-conn (db-conn/make-db-conn (assoc db-config :db-name "mysql"))
     :db-conn (db-conn/make-db-conn db-config)
     :worker-config {:db-config db-config
                     :worker-id :sample-worker
                     :table job-table
                     :sql-log? false}
     :default-job {:id 1 :priority 0 :attempts 0 :handler nil :last-error nil
                   :run-at nil :locked-at nil :failed-at nil :locked-by nil :queue nil}}))

(def sql-types {:int "int"
                :date-time "datetime"
                :text "varchar(255)"
                :long-text "longtext"})

(defn quasi-schema [fixtures schema]
  (let [
        tables (mapv (comp hyphen->underscore name) (keys fixtures))
        columns (letfn [(h->u [table row]
                          (let [cols (or (keys row) (keys (get schema table)))]
                            {:cols (mapv u/keyword->underscored-string cols)
                             :cols-defs (mapv (fn [col]
                                                (let [col-type (get sql-types (get-in schema [table col]))]
                                                  (assert (some? col-type) (str col " has no type"))
                                                  (str "`" (u/keyword->underscored-string col)
                                                       "` "
                                                       col-type)))
                                              cols)}))]
                  (map h->u (keys fixtures) (map #(get % 0) (vals fixtures))))]
    (zipmap tables columns)))

(defn create-tables-insert-rows [db-conn quasi-schema fixtures]
  (doseq [[table-name {:keys [cols cols-defs]}] quasi-schema]
    (let [;; cols-defs (mapv #(str  "`" % "` int") cols) ;mysql quoting of column names
          ;; table-id "table_name"
          ;; cols-defs (conj cols-defs (str table-id " varchar(255)"))
          table (keyword (underscore->hyphen table-name))
          rows (get fixtures (keyword (underscore->hyphen table-name)))
          rows (mapv (fn [row]
                       (vec (vals row))
                       ;; (conj (vec (vals row)) table-name)
                       )
                     rows)]
      (mysql/create-table db-conn  {:table-name table-name :columns cols-defs} {:quoting :off})
      (if (seq rows)
        (mysql/insert-rows db-conn {:table-name table-name :cols cols ;; (conj cols table-id)
                                    :rows rows})))))

(defn setup-test-db [db-conn test-db-conn {:keys [db-name schema]} fixtures]
  (mysql/drop-db db-conn {:db-name db-name})
  (mysql/create-db db-conn {:db-name db-name})
  (create-tables-insert-rows test-db-conn (quasi-schema fixtures schema) fixtures))

(defn make-job-fixtures [{:keys [default-job job-table] {:keys [schema]} :db-config} jobs]
  (let [all-keys (reduce (fn [ks job]
                           (apply conj ks (keys job))
                           ) #{} jobs)
        all-keys (if (seq all-keys) all-keys (keys (get schema job-table)))
        minimal-default-job (reduce (fn [j k]
                              (assoc j k (k default-job)))
                            {} all-keys)]
    (if-not (set/subset? (set all-keys) (set (keys default-job)))
      (throw (ex-info "unknown job columns!" {:all-keys all-keys})))
    {job-table (into [] (map-indexed #(merge minimal-default-job
                                             %2
                                             {:id (inc %1)})
                                     jobs))}))

(defn setup-job-test-db [{:keys [mysql-conn db-conn db-config] :as defaults} fixtures]
  (if (some? fixtures)
    (let [fixtures (make-job-fixtures defaults fixtures)]
      (setup-test-db mysql-conn
                     db-conn
                     db-config
                     fixtures)
      fixtures)))

(defn prepare-for-test [defaults fixtures]
  (let [worker (worker/make-worker (:worker-config defaults))
        env (worker/env worker)]
    {:env env
     :worker worker
     :fixtures (get (setup-job-test-db defaults fixtures) (:job-table defaults))}))

(defn prepare-for-test-merge-worker-config [defaults some-worker-config fixtures]
  (prepare-for-test (update defaults :worker-config
                            (fn [worker-config]
                              (merge worker-config some-worker-config)))
                    fixtures))

(defn adjust-tz [{:keys [table]
                  {:keys [schema]} :db-config} job]
  (let [schema (table schema)]
    (into {} (map (fn [[col v]]
                    [col (cond-> v
                           (and (some? v) (= :date-time (col schema)))
                           (time/to-time-zone (time/default-time-zone)))])
                  job))))

(defn job-table-data [{:keys [table] :as env}]
  (let [jobs (db/sql env :select-all-from {:table table})]
    (map (partial adjust-tz env)
         jobs)))

(defn test-logger
  ([env level text]
   (test-logger env nil level text))
  ([{:keys [worker-id log-atom log-ch]} {:keys [id name queue] :as job} level text]
   (let [queue-str (if queue (str ", queue=" queue))
         job-str (if job (str "Job " name " (id=" id queue-str ") " ))
         text (str "[" worker-id "] " job-str text)]
     (swap! log-atom #(conj % (str level "-" text))))))

;; (def u-now (u/now))
;; (pprint u-now)
(def u-now (time/date-time 2017 1 1 13 00 0))
;; (pprint u-now)
(def now (u/sql-time u-now))
(def a-minute-ago (time/minus now (time/minutes 1)))
(def two-minutes-ago (time/minus now (time/minutes 2)))
(def three-minutes-ago (time/minus now (time/minutes 3)))
(def five-hours-ago (time/minus now (time/hours 5)))

(defn mock-runtime
  "Evaluates expr and returns a map with the time it took in ms
  under :runtime and the result of the expression under :result "
  [f  & args]
  (let [result (apply f args)]
    {:runtime 10
     :result result}))

(defn <!!-status-change
  ([status-change-ch] (<!!-status-change status-change-ch (* 10 1000)))
  ([status-change-ch timeout]
   (let [timeout-ch (async/timeout timeout)]
     (async/alt!!
       status-change-ch ([v _] v)
       timeout-ch :timeout))))

(defn watch-worker-status [worker-status status-change-ch]
  (add-watch worker-status :status (fn [_ _ _ status]
                                     (async/put! status-change-ch status))))

(defn setup-worker-test [{:keys [worker-config job-records]}]
  (let [status-change-ch (async/chan 1 (filter #(contains? #{:stopped :timeout :done :crashed} %)))
        log-atom (atom [])
        {:keys [env worker fixtures]}
        (prepare-for-test-merge-worker-config defaults
                                              (merge {:logger test-logger}
                                                     worker-config
                                                     {:log-atom log-atom})
                                                 job-records)]

    (watch-worker-status (:worker-status env) status-change-ch)

    {:worker worker
     :env env
     :fixtures fixtures
     :<!!-status (partial <!!-status-change status-change-ch)
     :log-atom log-atom}))

(defn run-worker-once [worker-config job-records methods-called]
  (reset! methods-called [])
  (let [{:keys [env fixtures worker <!!-status log-atom]}
        (setup-worker-test
         {:worker-config (merge worker-config {:exit-on-complete? true})
          :job-records job-records})] ;this'll insert a default job
    (with-redefs [digicheck.common.util/exception-str
                  (fn [e] (str "Exception: " (.getMessage e)))]
      (worker/start worker)
      (loop []
        (let [status (<!!-status)]
          (if-not (contains? #{:stopped :timeout :done :crashed} status)
            (recur)
            {:log-atom @log-atom
             :methods-called @methods-called
             :status status
             :fixtures fixtures
             :job-table-data (job-table-data env)}))))))

(defn make-handler-yaml [{:keys [job-name payload]}]
  (str "--- !ruby/struct:" (str/strip-prefix (str job-name) ":") "\n"
       (yaml/generate-string payload)))
