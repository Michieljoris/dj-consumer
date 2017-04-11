(ns dj-consumer.fixtures
  (:require
   [clojure.test :refer [use-fixtures is deftest testing]]
   [dj-consumer.database.queries :as mysql]
   [dj-consumer.database.connection :as db-conn]
   [dj-consumer.database.core :as db]

   ;; String manipulation
   [cuerdas.core :as str]
   [dj-consumer.util :as u :refer [includes? transform-keys underscore->hyphen hyphen->underscore]]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log trace debug info warn error fatal report color-str
                logf tracef debugf infof warnf errorf fatalf reportf
                spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   ;; https://github.com/xsc/jansi-clj
   ;; (println "ERROR:" (underline "This" " is " "a message."))
   [jansi-clj.core :refer :all]))

(def sql-types {:int "int"
                :date-time "datetime"
                :text "varchar(255)"
                :long-text "longtext"})

(defn quasi-schema [fixtures schema]
  (let [tables (mapv (comp hyphen->underscore name) (keys fixtures))
        columns (letfn [(h->u [table row]
                          (let [cols (keys row)]
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
      (mysql/insert-rows db-conn {:table-name table-name :cols cols ;; (conj cols table-id)
                                  :rows rows}))))

(defn setup-test-db [db-conn test-db-conn {:keys [db-name schema]} fixtures]
  (mysql/drop-db db-conn {:db-name db-name})
  (mysql/create-db db-conn {:db-name db-name})
  (create-tables-insert-rows test-db-conn (quasi-schema fixtures schema) fixtures))

;; (defn make-db-fixture
;;   [{:keys [mysql-db-conn db-conn db-config] :as env} fixtures]
;;   (fn [test-fn]
;;     (setup-test-db mysql-db-conn db-conn (:db-name db-config) fixtures)
;;     (test-fn)))

;; (do

;;   ;; (create-tables-insert-rows nil (quasi-schema fixtures schema) fixtures)
;;   ;; Test use-fixtures
;;   (def fixtures {:job-table [{:priority 1 :handler nil :id 0 :last-error "bla"}]})

;;   (def db-config {:user "root"
;;                   :password ""
;;                   :url "//localhost:3306/"
;;                   :db-name "dj_consumer_test"
;;                   :schema  {:job-table {:priority :int
;;                                         :attempts :int
;;                                         :id :int
;;                                         :handler :long-text
;;                                         :last-error :long-text
;;                                         :run-at :date-time
;;                                         :locked-at :date-time
;;                                         :failed-at :date-time
;;                                         :locked-by :text
;;                                         :queue :text}}
;;                   })

;;   (def mysql-db-conn (db-conn/make-db-conn (assoc db-config :db-name "mysql")))
;;   (def test-db-conn (db-conn/make-db-conn db-config))
;;   (setup-test-db mysql-db-conn test-db-conn db-config fixtures)


;;   ;; (def env {:db-config (assoc db-config :db-name test-db-name)
;;   ;;           :mysql-db-conn mysql-db-conn
;;   ;;           :db-conn test-db-conn})

;;   ;; (use-fixtures :each (make-db-fixture env fixtures))
;;   )

;; (deftest test-fixtures
;;     (testing "Simple test to test if fixtures are working :)"
;;       (is (= (db/sql env :select-all-from {:table :main-table}) (:main-table fixtures)))))
