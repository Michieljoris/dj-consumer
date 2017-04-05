(ns dj-consumer.database.connection
  (:require
   [jdbc.pool.c3p0 :as pool]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]
   )
  )

(def db-conn (atom {}))

(defn make-subname [url db-name]
  (str url db-name "?zeroDateTimeBehavior=convertToNull"))

(defn make-db-conn [{:keys [user password url db-name pool print-spec min-pool-size initial-pool-size]}]
  (let [db-spec {:classname   "com.mysql.jdbc.Driver"
                 :subprotocol "mysql"
                                        ; This has zeroDateTimeBehaviour set for jdbc
                                        ; So that 0000-00-00 00:00:00 date times from the database
                                        ; are handled as null otherwise it will throw exceptions.
                 :subname  (make-subname url db-name)
                 :min-pool-size (or min-pool-size 3)
                 :initial-pool-size (or initial-pool-size 3)
                 :user  user
                 :password password}]
    (when print-spec
      (info "Database details:")
      (pprint (assoc db-spec :password "***")))
    (if pool
      (pool/make-datasource-spec db-spec)
      db-spec)))

(defn set-db-conn [some-db-conn]
  (reset! db-conn some-db-conn))

(defn set-db-conn-from-config [some-db-config]
  (set-db-conn (make-db-conn some-db-config)))

(defn init [{:keys [db-conn db-config]}]
  (if db-conn (set-db-conn db-conn) (set-db-conn-from-config db-config)))
