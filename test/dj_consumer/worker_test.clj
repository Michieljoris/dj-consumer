(ns dj-consumer.worker-test
  (:require
   [dj-consumer.database.core :as db]
   [dj-consumer.database.connection :as db-conn]
   [dj-consumer.fixtures :as fixtures]
   [clojure.test :as t :refer [deftest is use-fixtures testing]]

   [clj-time.core :as time]
   [clj-time.coerce :as time-coerce]
   [clj-time.local :as time-local]
   [dj-consumer.util :as u]
   [dj-consumer.worker :as tn]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]
   ))


(deftest default-logger
  (is (= 1 1
       ;; (tn/default-logger {:worker-id "some-worker" :table :job-table}
       ;;                    {:id 1 :name :some-job :queue "some-queue"} :info "some text")
       ;; "foo"
       )))

(defn make-job-fixtures [{:keys [default-job job-table]} jobs]
  (let [all-keys (reduce (fn [ks job]
                           (apply conj ks (keys job))
                           ) #{} jobs)
        minimal-default-job (reduce (fn [j k]
                              (assoc j k (k default-job)))
                            {} all-keys)]
    (if-not (clojure.set/subset? (set all-keys) (set (keys default-job)))
      (throw (ex-info "unknown job columns!" {:all-keys all-keys})))
    {job-table (into [] (map-indexed #(merge minimal-default-job
                                             %2
                                             {:id (inc %1)})
                                     jobs))}))

(defn setup-test-db [{:keys [mysql-conn db-conn db-config] :as defaults} fixtures]
  (let [fixtures (make-job-fixtures defaults fixtures)]
    (fixtures/setup-test-db mysql-conn
                            db-conn
                            db-config
                            fixtures)
    fixtures))

(defn job-table-data [{:keys [table] :as env}]
  (db/sql env :select-all-from {:table table}))

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
                     :sql-log? true}
     :default-job {:priority 0 :attempts 0 :handler nil :last-error nil
                   :run-at nil :locked-at nil :failed-at nil :locked-by nil :queue nil}}))

(defn prepare-for-test [defaults fixtures]
  (let [worker (tn/make-worker (:worker-config defaults))
        env (tn/env worker)]
    {:env env
     :worker worker
     :fixtures (get (setup-test-db defaults fixtures) (:job-table defaults))}))

;; (deftest test-fixtures
;;   (let [{:keys [env worker fixtures]}
;;         (prepare-for-test defaults [{:locked-at nil}])]
;;     ;; (testing "Simple test to test if fixtures are working :)"
;;     ;;   (is (= (job-table-data env) fixtures)))
;;     ))


;; (deftest clear-locks
;;   (let [{:keys [env worker fixtures]}
;;         (prepare-for-test defaults [{:locked-at (tn/now) :locked-by nil}])]
;;     (tn/clear-locks env)
;;     (is (job-table-data env) fixtures)))

(defn inst->str [x]
  (.format (java.text.SimpleDateFormat. "yyyy-MM-dd HH:mm:ss") x))

(deftest clear-locks
  (let [some-time (clj-time.core/date-time 1986 10 14 15 0 0 0)
        {:keys [env worker fixtures]}
        (prepare-for-test defaults [{:locked-at some-time :locked-by "sample-worker"}
                                    {:locked-at some-time :locked-by "sample-worker"}
                                    {:locked-at some-time :locked-by "sample-worker2"}
                                    ])]
    ;; (pprint env)
    (tn/clear-locks env)
    (is
     (=
      (job-table-data env)
      [{:locked-by nil, :locked-at nil, :id 1}
       {:locked-by nil, :locked-at nil, :id 2}
       {:locked-by "sample-worker2", :locked-at some-time :id 3}]))))

(deftest failed
  (let [some-time (clj-time.core/date-time 1986 10 14 15 0 0 0)
        {:keys [env worker fixtures]}
        (prepare-for-test defaults [{:locked-at some-time :locked-by "sample-worker"}
                                    {:locked-at some-time :locked-by "sample-worker"}
                                    {:locked-at some-time :locked-by "sample-worker2"}
                                    ])]
    ;; (tn/clear-locks env)
    ;; (is
    ;;  (=
    ;;   (job-table-data env)
    ;;   [{:locked-by nil, :locked-at nil, :id 1}
    ;;    {:locked-by nil, :locked-at nil, :id 2}
    ;;    {:locked-by "sample-worker2", :locked-at some-time :id 3}]))
    )

  )
;; (def t1
;;   (time/date-time 10 1 1))
;; (def t2
;;   (time/date-time 10 1 1))
;; (= t1 t2)

;; (time-coerce/to-sql-time)
