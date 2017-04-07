(ns dj-consumer.worker
  (:require
   [clojure.core.async
             :as a
             :refer [>! <! >!! <!! go go-loop chan buffer close! thread
                     alts! alts!! timeout]]
   [dj-consumer.database :as db]
   [dj-consumer.database.connection :as db-conn]
   [dj-consumer.util :as u]
   [dj-consumer.job :as job]

   [jdbc.pool.c3p0 :as pool]
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
               :max-run-time (* 3600 4) ;4 hours
               :reschedule-at (fn [now attempts] (int (+ now (Math/pow attempts 4))))
               :listen :false;or true/:poll or:binlog
               :destroy-failed-jobs  false
               :poll-interval 5 ;in seconds
               :table :delayed-job
               :queues nil ;queues to process, nil processes all.
               })

(defn reserve-job [{:keys [table worker-id] :as env}]
  (let [now (t/now)
        lock-job-scope (db/make-lock-job-scope env now)
        ;;Lock a job record
        locked-job-count (db/sql env :update-record lock-job-scope)]
    (if (pos? locked-job-count)
      (let [now (u/to-sql-time-string now)
            query-params (db/make-query-params {:table table
                                             :where [:and [[:locked-at := now]
                                                           [:locked-by := (str worker-id)]
                                                           [:failed-at :is :null]]]})
            ;;Retrieve locked record by
            record (first (db/sql env :get-cols-from-table query-params))]
        (assoc record :handler (u/parse-ruby-yaml (:handler record)))))))


(defprotocol IWorker
  (start [this])
  (stop [this]))

(defrecord Worker [env stop-ch]
  IWorker
  (start [this]
    (info "Starting worker")
    (pprint (dissoc (reserve-job env) :handler))
    )
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
                     :reserve-scope (db/make-reserve-scope env))
              (chan))))

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
