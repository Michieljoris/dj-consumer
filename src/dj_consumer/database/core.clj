(ns dj-consumer.database.core
  (:require [bilby.database.clauses :as db-clauses]
            [clj-time.core :as t]
            [clj-time.format :as time-format]
            [cuerdas.core :as str]
            [digicheck.common.util :as u]
            [clj-time.jdbc] ;;IMPORTANT: deals with sql datetime. Don't remove!!!!
            [jansi-clj.core :refer :all]
            [taoensso.timbre :as timbre :refer [info]]))

(def sql-formatter (time-format/formatter "yyyy-MM-dd HH:mm:ss"))

(defn to-sql-time-string [t]
   (time-format/unparse sql-formatter t))

;;TODO: there's a table-name fb in bilby-libs/bilby.database.inspect. But it's not
;;compatible with this one.
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
                         u/hyphen->underscore)
    (string? table) table))

(defn make-query-params
  "Takes all the elements need to build and query and returns a data
  map ready to pass to a hugsql query"
  [env {:keys [table cols where limit order-by updates]}]
  (let [table-name (table-name env table)]
    {:table table
     :cols cols
     :updates updates
     :where-clause (if where (db-clauses/conds->sqlvec {:table table
                                                        :table-name table-name
                                                        :alias-prefix ""
                                                        :props nil
                                                        :cols (db-clauses/conds->sqlvec {:table table
                                                                                         :table-name table-name
                                                                                         :alias-prefix ""
                                                                                         :props nil
                                                                                         :cols nil
                                                                                         :conds where})
                                                        :conds where}))
     :limit-clause (if limit (db-clauses/make-limit-clause limit nil))
     :order-by-clause (if order-by (db-clauses/order-by->sqlvec {:table table
                                                                 :table-name table-name
                                                                 :alias-prefix ""
                                                                 :cols nil
                                                                 :order-by order-by}))}))

(defn make-reserve-scope
  "Takes an environment and returns a map that defines the scope that
  finds one suitable job."
  [{:keys [worker-id table queues min-priority max-priority] :as env}]
  {:pre [(string? worker-id)]}
  (let [nil-queue? (contains? (set queues) nil)
        queues (remove nil? queues)
        run-at-before-marker "run-at-before"
        locked-at-before-marker "locked-at-before"]
    (make-query-params env {:table table
                            ;; From delayed_job_active_record ruby gem:
                            ;; (run_at <= ? AND (locked_at IS NULL OR locked_at < ?) OR locked_by = ?) AND failed_at IS NULL"
                            :where [:and (into [[:failed-at :is :null]
                                                [:or [[:and [[:run-at :<= run-at-before-marker]
                                                             [:or [[:locked-at :is :null]
                                                                   [:locked-at :< locked-at-before-marker]]]]]
                                                      [:locked-by := worker-id]]]]
                                               (remove nil?
                                                       [(if min-priority [:priority :>= min-priority])
                                                        (if max-priority [:priority :<= max-priority])
                                                        (if (seq queues)
                                                          (if nil-queue?
                                                            [:or [[:queue :in queues] [:queue :is :null]]]
                                                            [:queue :in queues])
                                                          (if nil-queue? [:queue :is :null]))]))]
                            :limit {:count 1}
                            :order-by [[:priority :asc] [:run-at :asc]]})))

(defn make-lock-job-scope
  "Takes an environment and now and returns a time dependent scope
  that returns a suitable job for now. Adds updates clause that locks
  the record."
  [{:keys [worker-id max-run-time reserve-scope] :as env} now]
  {:pre [(string? worker-id)]}
  (let [now-minus-max-run-time (t/minus now (t/seconds max-run-time))
        now-minus-max-run-time (to-sql-time-string now-minus-max-run-time)
        now (to-sql-time-string now)
        reserve-scope (update reserve-scope :where-clause
                              #(mapv (fn [e] (condp = e
                                               "run-at-before" now
                                               "locked-at-before" now-minus-max-run-time
                                               e)) %))]
    (merge reserve-scope {:updates {:locked-by worker-id
                                    :locked-at now}})))

;;TODO: use sql fn from bilby.database.query in bilby-parser
(defn sql
  "Executes fun with db connection as second argument "
  [env fun params]
  (let [cols (mapv (comp u/hyphen->underscore name) (:cols params))
        params (condp = fun
                 :select-all-from
                 (assoc params
                        :table (table-name env (:table params))
                        )
                 :get-cols-from-table
                 (assoc params
                        :table (table-name env (:table params))
                        :cols cols)
                 :now {}
                 :update-record
                 (assoc params
                        :table (table-name env (:table params))
                        :updates
                        (u/transform-keys (comp keyword u/hyphen->underscore name) (:updates params))
                        )
                 :delete-record
                 (assoc params
                        :table (table-name env (:table params))
                        )
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
        (u/transform-keys (comp keyword u/underscore->hyphen name) result)))))

;; (def env {:sql-log? true
;;           :table :delayed-job
;;           :db-conn (db-conn/make-db-conn {:user "root"
;;                                           :password ""
;;                                           :url "//localhost:3306/"
;;                                           :db-name "chin_minimal"
;;                                           ;; :db-name "chinchilla_development"
;;                                           })})
;; (sql env :update-record {:table :delayed-job :updates {:failed-at (to-sql-time-string (t/now))}})
;; (do
;;   (def r (sql env :get-cols-from-table  (make-query-params env {:table :delayed-job
;;                                                             :cols [:id :failed-at :run-at]
;;                                                             :where [:run-at :< (to-sql-time-string (t/now))]})))
;;   (pprint (map #(select-keys % [:id :failed-at :run-at]) r)))
