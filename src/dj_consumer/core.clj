(ns dj-consumer.core
  (:require
   [clojure.core.async
             :as a
             :refer [>! <! >!! <!! go go-loop chan buffer close! thread
                     alts! alts!! timeout]]
   [dj-consumer.database :as db]
   [dj-consumer.database.clauses :as cl]
   [dj-consumer.database.connection :as db-conn]
   [dj-consumer.util :as u]
   [dj-consumer.job :as job]

   [jdbc.pool.c3p0 :as pool]
   [yaml.core :as yaml]
   [clj-time.core :as t]

   ;; String manipulation
   [cuerdas.core :as str]

   [dj-consumer.sample-job]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]))

(def defaults {:sql-log? false
               :max-attempts 25
               :min-priority 0
               :max-priority 10
               :read-ahead 4
               :max-run-time (* 3600 4) ;4 hours
               :reschedule-at (fn [now attempts] (int (+ now (Math/pow attempts 4))))
               :listen :false;or true/:poll or:binlog
               :destroy-failed-jobs  false
               :poll-interval 5 ;in seconds
               :job-table :delayed-job
               :queues nil ;queues to process, nil processes all.
               })

(defn make-query-params [{:keys [table cols where limit order-by]}]
  {:table table
   :cols cols
   :where-clause (if where (cl/conds->sqlvec table "" nil (cl/conds->sqlvec table "" nil nil where) where))
   :limit-clause (if limit (cl/make-limit-clause limit))
   :order-by-clause (if order-by (cl/order-by->sqlvec table "" nil order-by))})

(defn make-reserve-scope [{:keys [worker-id table queues min-priority max-priority max-run-time cols]}]
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
                                                  [:locked-by := (str worker-id)]]]]
                                           (remove nil?
                                                   [(if min-priority [:priority :>= min-priority])
                                                    (if max-priority [:priority :>= max-priority])
                                                    (if (seq queues)
                                                      (if nil-queue?
                                                        [:or [[:queue :in queues] [:queue :is :null]]]
                                                        [:queue :in queues])
                                                      (if nil-queue? [:queue :is :null]))]))]
                        :order-by-clause [[:priority :asc] [:run-at :asc]]})))

(defn make-lock-job-scope [{:keys [worker-id max-run-time reserve-scope] :as env}]
  (let [now (t/now)
        now-minus-max-run-time (t/minus now (t/seconds max-run-time))
        now-minus-max-run-time (u/to-sql-time-string now-minus-max-run-time)
        now (u/to-sql-time-string now)
        reserve-scope (update reserve-scope :where-clause
                              #(mapv (fn [e] (condp = e
                                               "run-at-before" now
                                               "locked-at-before" now-minus-max-run-time
                                               e)) %))]
    (merge reserve-scope {:updates {:locked-by worker-id
                                    :locked-at now}
                          :limit {:count 1}})))

;; (defn reserve-job [env]
;;   (let [lock-job-scope (make-lock-job-scope env)
;;         locked-job-count (db/sql env :update-record lock-job-scope)
;;         ]
;;     (if (pos? locked-job-count)
;;       ;;TODO
;;       ;; where(locked_at: now, locked_by: worker.name, failed_at: nil).first

;;       ;(map (fn [{:keys [handler] :as record}] (assoc record :handler (parse-ruby-yaml handler))))
;;       :returning-job)
;;     ))

(defprotocol IWorker
  (start [this])
  (stop [this]))

(defrecord Worker [config]
  IWorker
  (start [this] (info "Starting worker"))
  (stop [this]))

(defn make-worker[{:keys [db-conn db-config] :as env}]
  {:pre [(some? (:worker-id env))
         (some? (or (:db-conn env) (:db-config env)))]}
  (let [env (merge defaults env)]
    (when (:verbose env)
      (info "Initializing dj-consumer with:")
      (pprint env))
    (->Worker (assoc env
                     :db-conn (or db-conn (db-conn/make-db-conn db-config))
                     :reserve-scope (make-reserve-scope env)
                     ))))

(defn start-worker [input-ch stop-ch]
  (go-loop []
    (let [[v ch] (alts! [input-ch stop-ch])]
      (if (identical? ch input-ch)
        (do
          (info "Value on input-ch is: " v)
          (recur))))))

(def input-ch (chan))
(def stop-ch (chan))
;; (close! stop-ch)
;; (close! input-ch)

;; (start-worker input-ch stop-ch)
;; (>!! input-ch "hello")
;; (a/put! input-ch "hello")
;; (a/put!  stop-ch "hello")


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
        method-name (:method_name data)
        object-method (str object-name method-name)]
    {:job (or (u/camel->keyword struct-name)
              (if (and object-name method-name) (u/camel->keyword object-name method-name))
              :unknown-job-id)
     :data data}))

(def job-queue (atom []))

;timeout, async, lock table, all in one sql transaction?,
(defn try-job [{:keys [locked-at locked-by failed-at] :as job-record}]
  (try
    :foo
    (catch Exception e :foo))
  )

;; (defn process-jobs [jobs]

;;   )



;; AILS_ENV=production script/delayed_job start
;; RAILS_ENV=production script/delayed_job stop

;; # Runs two workers in separate processes.
;; RAILS_ENV=production script/delayed_job -n 2 start
;; RAILS_ENV=production script/delayed_job stop

;; # Set the --queue or --queues option to work from a particular queue.
;; RAILS_ENV=production script/delayed_job --queue=tracking start
;; RAILS_ENV=production script/delayed_job --queues=mailers,tasks start

;; # Use the --pool option to specify a worker pool. You can use this option multiple times to start different numbers of workers for different queues.
;; # The following command will start 1 worker for the tracking queue,
;; # 2 workers for the mailers and tasks queues, and 2 workers for any jobs:
;; RAILS_ENV=production script/delayed_job --pool=tracking --pool=mailers,tasks:2 --pool=*:2 start

;; # Runs all available jobs and then exits
;; RAILS_ENV=production script/delayed_job start --exit-on-complete
;; # or to run in the foreground
;; RAILS_ENV=production script/delayed_job run --exit-on-complete

(do
  (let [worker (make-worker{:worker-id :sample-worker
                            :sql-log? true
                            :db-config {:user "root"
                                        :password ""
                                        :url "//localhost:3306/"
                                        :db-name "chin_minimal"
                                        ;; :db-name "chinchilla_development"
                                        }

                            })]
    ;; (worker :start)
    (start worker)
    )

  ;; (pprint (db/sql :select-all-from {:table :delayed-job}))
  ;; (pprint (db/sql :get-cols-from-table {:table :delayed-job :cols [:id :handler]
  ;;                                       :where-clause
  ;;                                       (cl/conds->sqlvec :delayed-job "" nil [:id] [:id := 2988200])
  ;;                                       }))
  ;; (pprint handler-str)

  ;; (def handler-str (-> (db/sql :get-cols-from-table {:table :delayed-job :cols [:id :handler]
  ;;                                                    :where-clause
  ;;                                                    (cl/conds->sqlvec :delayed-job "" nil [:id] [:id := 2988200])
  ;;                                                    })
  ;;                      first
  ;;                      :handler))
  ;; (job/perform :user/say-hello {:id 1} nil nil)
  ;; (job/after :user/say-hello {:id 1})
  ;; (job/perform :invitation-expiration-reminder-job nil nil nil)
  ;; (pprint handler-str)
  ;; (pprint (parse-ruby-yaml handler-str))
  ;; (pprint (retrieve-jobs (merge @config {:now (t/now)})))
  )
