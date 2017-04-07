(ns dj-consumer.database
  (:require
   [dj-consumer.util :as u]

   [dj-consumer.database.queries]
   [dj-consumer.database.connection :as db-conn]
   [dj-consumer.database.info :as db-info]
   ;; [clj-time.jdbc]

   ;; String manipulation
   [cuerdas.core :as str]

   [clj-time.core :as t]
   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]
   )
  )

(defn sql
  "Executes fun with db connection as second argument "
  [env fun params]
  (let [cols (mapv (comp u/hyphen->underscore name) (:cols params))
        params (condp = fun
                 :select-all-from
                 (assoc params
                        :table (db-info/table-name (:table params))
                        )
                 :get-cols-from-table
                 (assoc params
                        :table (db-info/table-name (:table params))
                        :cols cols)
                 :now {}
                 :update-record
                 (assoc params
                        :table (db-info/table-name (:table params))
                        :updates
                        (u/transform-keys (comp keyword u/hyphen->underscore name) (:updates params))
                        )
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
        fun-str (str "dj-consumer.database.queries/" (name fun))
        fun-ref (resolve (symbol fun-str))]
    (if (:sql-log? env)
      (let [s (str fun-str "-sqlvec")
            fun-sqlvec (resolve (symbol s))
            sqlvec (fun-sqlvec params)
            sql-str (str/replace (first sqlvec) "\n" " ")
            sql-str (str/replace sql-str "  " " ")
            sql-seq (conj (rest sqlvec) sql-str)]
        (info (green (str/join " " sql-seq)))))
    (let [result (fun-ref (:db-conn env) params)]
      (condp = fun
        :now (first (vals (first result)))
        ;; http://www.hugsql.org/#using-insert
        ;; :insert-record (:generated_key result) ;NOTE: generated_key  only works for mysql
        ;; :insert-record-no-ts (:generated_key result) ;NOTE: generated_key  only works for mysql
        ;; :delete-record (if (= 1 result) result) ;return nil if nothing is deleted
        (u/transform-keys (comp keyword u/underscore->hyphen name) result))
      )))

;; (def n (sql :now nil))
;; (pprint n)
;; (def n2 (t/now))
;; (pprint n2)
;; (t/plus n2 (t/seconds 3600))
;; ;
;;                                         => #inst "2017-04-06T23:15:07.000000000-00:00"
(def env {:sql-log? true
          :db-conn (db-conn/make-db-conn {:user "root"
                                          :password ""
                                          :url "//localhost:3306/"
                                          :db-name "chin_minimal"
                                          ;; :db-name "chinchilla_development"
                                          })})
;; (sql env :now nil)
;; (sql env :update-record {:job-table :delayed-job :updates {:priority 1 :queue "foo"}})
