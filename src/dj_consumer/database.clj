(ns dj-consumer.database
  (:require
   [dj-consumer.config :refer [config]]
   [dj-consumer.util :as u]

   [hugsql.core :as hugsql]
   [yesql.core :as yesql]

   [jdbc.pool.c3p0 :as pool]

   ;; String manipulation
   [cuerdas.core :as str]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]
   )
  )

(def sql-file "dj_consumer/hugsql.sql" )

;; Load all sql fns into this namespace
(hugsql/def-db-fns sql-file  {:quoting :mysql})
(hugsql/def-sqlvec-fns sql-file ;; {:quoting :mysql}
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

(def db-info {})

(defn table-name
  "Table names are singular hyphenated and keywords. This fn returns the actual table name
  by looking it up in db-config or otherwise just adds an 's'. Returns a string or nil."
  [table]
  (if (some? table)
    (let [table (keyword table)]
      (-> (or (get-in db-info [table :table-name]) (str (name table) "s"))
          name
          u/hyphen->underscore))))

(defn sql
  "Executes fun with db connection as second argument "
  [fun params]
  (let [cols (mapv (comp u/hyphen->underscore name) (:cols params))
        params (condp = fun
                 :select-all-from
                 (assoc params
                        :table (table-name (:table params))
                        )
                 ;; :get-cols-from-table
                 ;; (assoc params
                 ;;        :table (table-name (:table params))
                 ;;        :cols cols)
                 ;; :get-joined-rows
                 ;; ;; When t1==t2 aliases kick in, see admin.sql
                 ;; (let [t1 (:t1 params)
                 ;;       t2 (:t2 params)
                 ;;       t1-name (table-name t1)
                 ;;       t2-name (table-name t2)
                 ;;       t1=t2? (= t1 t2)
                 ;;       [t1-alias t2-alias] (if t1=t2?
                 ;;                             [(str "t1_" t1-name) (str "t2_" t2-name)]
                 ;;                             [t1-name t2-name])
                 ;;       t1-foreign-key (keyword->underscored-string (:t1-foreign-key params))
                 ;;       t2-foreign-key (keyword->underscored-string (:t2-foreign-key params))
                 ;;       cols (map #(str (if t1=t2? t2-alias t2-name) "." %) cols)
                 ;;       cols (conj cols (str (:join-table params) "." t1-foreign-key))
                 ;;       ]
                 ;;   (assoc params
                 ;;          :t1-name t1-name :t1-alias t1-alias
                 ;;          :t2-name t2-name :t2-alias t2-alias
                 ;;          :t1=t2? t1=t2?
                 ;;          :cols cols
                 ;;          :t1-foreign-key t1-foreign-key
                 ;;          :t2-foreign-key t2-foreign-key))
                 ;; :insert-record {:table (table-name (:table params)) :cols cols :vals (:vals params)
                 ;;                 :fun  (if (no-timestamp? (:table params)) :insert-record-no-ts)}
                 ;; :update-record {:table (table-name (:table params)) :id (:id params)
                 ;;                 :cols cols :vals (:vals params)
                 ;;                 :fun (if (no-timestamp? (:table params)) :update-record-no-ts)}
                 ;; :delete-record {:table (table-name (:table params)) :id (:id params)}
                 ;; :count-belongs-to {:table (table-name (:table params))
                 ;;                    :belongs-to-column (keyword->underscored-string (:belongs-to-column params))
                 ;;                    :id (:id params)
                 ;;                    :cond (:cond params)}
                 ;; :remove-child-dossier-type-id params
                 (throw (ex-info"Unknown sql function" {:fun fun})))
        fun (or (:fun params) fun)      ;if we decided to use a alternative fun, use that
        fun-str (str "dj-consumer.database/" (name fun))
        fun-ref (resolve (symbol fun-str))]
    (if (:sql-log config)
      (let [s (str fun-str "-sqlvec")
            fun-sqlvec (resolve (symbol s))
            sqlvec (fun-sqlvec params)
            sql-str (str/replace (first sqlvec) "\n" " ")
            sql-str (str/replace sql-str "  " " ")
            sql-seq (conj (rest sqlvec) sql-str)]
        (info (green (str/join " " sql-seq)))))
    (let [result (fun-ref @db-conn params)]
      (condp = fun
        ;; http://www.hugsql.org/#using-insert
        ;; :insert-record (:generated_key result) ;NOTE: generated_key  only works for mysql
        ;; :insert-record-no-ts (:generated_key result) ;NOTE: generated_key  only works for mysql
        ;; :delete-record (if (= 1 result) result) ;return nil if nothing is deleted
        (u/transform-keys (comp keyword u/underscore->hyphen name) result))
      )))
