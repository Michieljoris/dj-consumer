(ns dj-consumer.worker
  (:require [clj-time.core :as time]
            [clojure.core.async :as async]
            [clojure.pprint :refer [pprint]]
            [cuerdas.core :as str]
            [dj-consumer
             [reserve-and-run :as rr]
             [util :as u]]
            [dj-consumer.database
             [connection :as db-conn]
             [core :as db]]
            [taoensso.timbre :as timbre :refer [info]]))

;;TODO: last-error set it properly on job

;; Catch exceptions in threads. Otherwise they disappear and we'd never know about them!!
 ;; https://stuartsierra.com/2015/05/27/clojure-uncaught-exceptions
(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread ex]
     (timbre/error ex "Uncaught exception on" (.getName thread)))))

(def defaults {:table :delayed-job

               ;;Worker behaviour
               :max-attempts 25
               :delete-failed-jobs? false
               :on-reserve-fail :stop ;or :throw
               :max-failed-reserve-count 10
               :exit-on-complete? false
               :poll-interval 5 ;in seconds
               :reschedule-at (fn [some-time attempts]
                                (time/plus some-time (time/seconds (Math/pow attempts 4))))

               ;;Job selection:
               :min-priority nil
               :max-priority nil
               :queues nil ;nil is all queues, but nil is also a valid queue, eg [nil "q"]

               ;;Reporting
               :job-batch-size 100 ;report success/fail every so many jobs
               :verbose? false
               :logger rr/default-logger
               :sql-log? false
               })

(defn stop-worker
  "Set worker status to :stopped"
  [{:keys [worker-status] :as env}]
  (reset! worker-status :stopped))

(defn start-worker
  "Start a async thread and loops till worker status changes to not running. In
  the loop first run a batch of jobs. Log the result if any jobs were done. If
  none were done, set worker status to :stopped if exit-on-complete? flag is
  set. Then, if worker status is :running sleep for poll-interval seconds and
  then recur loop. Otherwise clear locks and exit async thread"
  [{:keys [logger poll-interval exit-on-complete? worker-status] :as env}]
  ;;If jvm crashed or was halted a job might have been locked still, this worker
  ;;would not pick up the job till at least max-run-time had expired
  (rr/clear-locks env)
  (reset! worker-status :running)
  (async/go-loop []
    (if (= @worker-status :running)
      (let [{:keys [runtime]
             {:keys [success fail]} :result} (u/runtime rr/run-job-batch env)
            total-runs (+ success fail)
            jobs-per-second (/ total-runs (/ runtime 1000.0))
            exit-on-complete? (and (zero? total-runs) exit-on-complete?)]
        (if (pos? total-runs)
          (logger env :info (str total-runs " jobs processed at "
                                 (format "%.2f" jobs-per-second)
                                 " jobs per second. " fail " failed.")))
        (when (= @worker-status :running)
          (if exit-on-complete?
            (do
              (logger env :info "No more jobs available. Exiting")
              (reset! worker-status :done))
            (do
              (when (zero? total-runs)
                (reset! worker-status :sleeping)
                (Thread/sleep poll-interval)
                (if (= @worker-status :sleeping)
                  (reset! worker-status :running)))
              (recur)))))
      (logger env :info "Stopped"))))

(defprotocol IWorker
  (start [this])
  (stop [this])
  (status [this])
  (env [this]))

(defrecord Worker [env]
  IWorker
  (start [this]
    (let [{:keys [logger log-atom]} env]
      (logger env :info "Starting")
      (let [{:keys [worker-status]} env]
        (if (contains? #{:running :sleeping} @worker-status)
          (logger env :info "Worker was already running")
          (start-worker env)
          ))))
  (stop [this]
    (let [{:keys [worker-status logger]} env]
      (logger env :info "Stopping")
      (if (contains? #{:stopped :crashed} @worker-status)
        (logger env :info "Worker was already not running")
        (stop-worker env))))
  (status [this] @(:worker-status env))
  (env [this] env))

(defn make-worker
  "Creates a worker that processes delayed jobs found in the db on a
  separate thread. Each worker is independant from other workers and
  can be run in parallel.Just make sure a running worker has a unique
  worker-id."
  [{:keys [worker-id verbose? db-conn db-config max-failed-reserve-count on-reserve-error] :as env}]
  {:pre [(some? worker-id)
         (or (nil? on-reserve-error) (contains? #{:stop :throw} on-reserve-error))
         (or (nil? max-failed-reserve-count) (number? max-failed-reserve-count))
         (some? (or db-conn db-config))]}
  (let [{:keys [db-conn db-config poll-interval worker-id] :as env} (merge defaults env)
        env (assoc env
                   :worker-id (str/strip-prefix (str worker-id) ":")
                   :poll-interval (* 1000 poll-interval)
                   :db-conn (or db-conn (db-conn/make-db-conn db-config)))]
    (when (:verbose? env)
      (info "Initializing dj-consumer with:")
      (pprint env))
    (->Worker (assoc env
                     :reserve-scope (db/make-reserve-scope env)
                     :failed-reserved-count (atom 0)
                     :max-run-time (* 3600 4) ;4 hours, hardcoded for every worker!!!!
                     :worker-status (atom :new)))))


;; (do
;;   (let [worker (make-worker{:worker-id :sample-worker
;;                             :sql-log? true
;;                             :verbose true
;;                             :db-config {:user "root"
;;                                         :password ""
;;                                         :url "//localhost:3306/"
;;                                         :db-name "chin_minimal"
;;                                         ;; :db-name "chinchilla_development"
;;                                         }

;;                             })]
;;     ;; (worker :start)
;;     (start worker)
;;     ;; (pprint (env worker))
;;     )

;;   ;; (pprint (db/sql :select-all-from {:table :delayed-job}))
;;   ;; (pprint (db/sql :get-cols-from-table {:table :delayed-job :cols [:id :handler]
;;   ;;                                       :where-clause
;;   ;;                                       (cl/conds->sqlvec :delayed-job "" nil [:id] [:id := 2988200])
;;   ;;                                       }))
;;   ;; (pprint handler-str)

;;   ;; (def handler-str (-> (db/sql :get-cols-from-table {:table :delayed-job :cols [:id :handler]
;;   ;;                                                    :where-clause
;;   ;;                                                    (cl/conds->sqlvec :delayed-job "" nil [:id] [:id := 2988200])
;;   ;;                                                    })
;;   ;;                      first
;;   ;;                      :handler))
;;   ;; (job/run:user/say-hello {:id 1} nil nil)
;;   ;; (job/after :user/say-hello {:id 1})
;;   ;; (job/run :invitation-expiration-reminder-job nil nil nil)
;;   ;; (pprint handler-str)
;;   ;; (pprint (parse-ruby-yaml handler-str))
;;   ;; (pprint (retrieve-jobs (merge @config {:now (now)})))
;;   )

;; (try
;;   (/ 3 0)
;;   (catch Exception e
;;     ;; (default-logger {:worker-id "foo-worker"} {:queue "baz-q" :id 1 :name "bar-job"} :info
;;     ;;                 (str (.toString e) "\nStacktrace:\n" (with-out-str (pprint (.getStackTrace e)))))
;;     ;; (throw e)
;;     (pprint (.getMessage e))
;;     ;; (pprint (.getCause e))
;;     (info (.toString e))
;;     ;; (info (with-out-str (pprint (.getStackTrace e))))
;;     ;; (pprint "blue")
;;     )
;;   (finally
;;     (pprint "finally")))


;; (do
;;   (let [c (chan)]
;;     (go
;;       (Thread/sleep 1000)
;;       (throw (ex-info "123" {}))
;;       (>! c :ok)
;;       )

;;     (pprint (<!! c)))
;;   )

;; (defn my-error []

;;   (Thread/sleep 4000)
;;   (throw (Exception. "from job"))
;;   (Thread/sleep 1000)
;;   (pprint "end")
;;   )
;; (def fut (future-call my-error))
;; (deref fut)

;; (thread (my-error))
;; (go (my-error))

;; (u/throwing-timeout my-error 2000)

;; (do
;;   (defn time-hogger []
;;     (loop [n 1]
;;       (/ n (+ 1 1.0))
;;       (if (and (not (Thread/interrupted)) (< n 2000000000))
;;         (recur (inc n)))
;;       )
;;     ;; (dotimes [n 10000000000]
;;     ;;   (/ n (+ n 1.0))
;;     ;;   )
;;     ;; (info (Thread/interrupted))
;;     ;; (info (Thread/interrupted))
;;     (if (Thread/interrupted)
;;       (pprint "interrupted!!")
;;       (pprint "Hello"))
;;     )
;;   (time (time-hogger)))

;; (u/throwing-timeout time-hogger 2000)


;; (defn throwing-timeout [env job f]
;;   (try
;;     (let [max-run-time 4000]
;;       (when (= (deref (future-call f) max-run-time :timeout
;;                       ) :timeout)
;;         (throw (ex-info (str "Job foo with name " (:name job) " has timed out.") {:timeout? true}))))
;;     (catch Exception e
;;       (let [{:keys [msg context]} (parse-ex-info e)]
;;         (cond
;;           (:timeout? context) (throw e)
;;           (.getCause e) (throw (.getCause e))
;;           :else (throw e)))

;;       )
;;     ))

;; (do
;;   ; http://stackoverflow.com/questions/11520394/why-do-cancelled-clojure-futures-continue-using-cpu/14540878#14540878


;;   ;; (throwing-timeout {} {:name "some-job"} (fn []
;;   ;;                                   (info "hello")
;;   ;;                                   (my-error)
;;   ;;                                   ))
;;   )


;;         (info (exception-str e))

;;         (info (exception-str (.getCause e)))
;;         (info (.toString e))
;;         (info (.toString (.getCause e)))
;;         (info (exception-str e))

;; (defn start-poll [env stop-ch]
;;   (go-loop []
;;     )
;;   )

;; (defn start-poll
;;   [env f time-in-ms]
;;   (let [stop (chan)]
;;     (go-loop []
;;       (alt!
;;         (timeout (:poll-interval env)) (do (<! (thread (f)))
;;                                  (recur))
;;         stop :stop))
;;     stop))


;; (defn start-worker [input-ch stop-ch]
;;   (go-loop []
;;     (let [[v ch] (alts! [input-ch stop-ch])]
;;       (if (identical? ch input-ch)
;;         (do
;;           (info "Value on input-ch is: " v)
;;           (recur))))))

;; (defn start [env stop-ch]
;;   (go-loop
;;       ))
;; (async/go-loop [i 0]
;;   (info "in go loop")
;;   (throw (Exception. "bla"))
;;   (if (< i 10)
;;     (recur (inc i))))
;; (into [] (filter even?) [1 2 3 4 5])
;; (filter even?)
