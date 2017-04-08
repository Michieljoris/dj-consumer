(ns dj-consumer.database
  (:require
   [dj-consumer.util :as u]

   [dj-consumer.database.clauses :as cl]
   [dj-consumer.database.queries]
   [dj-consumer.database.connection :as db-conn]
   [dj-consumer.database.info :as db-info]
   [clj-time.jdbc]

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

(defn make-query-params
  "Takes all the elements need to build and query and returns a data
  map ready to pass to a hugsql query"
  [{:keys [table cols where limit order-by updates]}]
  {:table table
   :cols cols
   :updates updates
   :where-clause (if where (cl/conds->sqlvec table "" nil (cl/conds->sqlvec table "" nil nil where) where))
   :limit-clause (if limit (cl/make-limit-clause limit))
   :order-by-clause (if order-by (cl/order-by->sqlvec table "" nil order-by))})

(defn make-reserve-scope
  "Takes an environment and returns a map that defines the scope that
  finds one suitable job."
  [{:keys [worker-id table queues min-priority max-priority max-run-time cols]}]
  (let [nil-queue? (contains? (set queues) nil)
        queues (remove nil? queues)
        run-at-before-marker "run-at-before"
        locked-at-before-marker "locked-at-before"]
    (make-query-params {:table table
                        :cols cols
                        ;; From delayed_job:
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
  (let [now-minus-max-run-time (t/minus now (t/seconds max-run-time))
        now-minus-max-run-time (u/to-sql-time-string now-minus-max-run-time)
        now (u/to-sql-time-string now)
        reserve-scope (update reserve-scope :where-clause
                              #(mapv (fn [e] (condp = e
                                               "run-at-before" now
                                               "locked-at-before" now-minus-max-run-time
                                               e)) %))]
    (merge reserve-scope {:updates {:locked-by worker-id
                                    :locked-at now}})))

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

;; (def n (sql :now nil))
;; (pprint n)
;; (def n2 (t/now))
;; (pprint n2)
;; (t/plus n2 (t/seconds 3600))
;; ;
;;                                         => #inst "2017-04-06T23:15:07.000000000-00:00"
;; (def env {:sql-log? true
;;           :table :delayed-job
;;           :db-conn (db-conn/make-db-conn {:user "root"
;;                                           :password ""
;;                                           :url "//localhost:3306/"
;;                                           :db-name "chin_minimal"
;;                                           ;; :db-name "chinchilla_development"
;;                                           })})
;; (sql env :now nil)
;; (sql env :update-record {:table :delayed-job :updates {:failed-at (u/to-sql-time-string (t/now))}})
;; (do
;;   (def r (sql env :get-cols-from-table  (make-query-params {:table :delayed-job
;;                                                             :cols [:id :failed-at :run-at]
;;                                                             :where [:run-at :< (u/to-sql-time-string (t/now))]})))
;;   (pprint (map #(select-keys % [:id :failed-at :run-at]) r)))

;; (unlock-job env {:id 2})
