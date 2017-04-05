(ns dj-consumer.core
  (:require
   [dj-consumer.database :as db]
   [dj-consumer.database.clauses :as cl]
   [dj-consumer.database.connection :as db-conn]
   [dj-consumer.config :refer [config]]
   [dj-consumer.util :as u]
   [dj-consumer.job :as job]

   [jdbc.pool.c3p0 :as pool]
   [yaml.core :as yaml]

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

(def defaults {:sql-log false
               :max-attempts 25
               :max-run-time (* 3600 4) ;4 hours
               :reschedule-at (fn [now attempts] (int (+ now (Math/pow attempts 4))))
               :listen :false;or true/:poll or:binlog
               :destroy-failed-jobs  false
               :poll-interval 5 ;in seconds
               :job-table :delayed-job
               :queues nil ;queues to process, nil processes all.

               })

(defn default-job [job-record]
  (error "No job defined!!!"))

(defn init [{:keys [db-conn db-config] :as some-config}]
  (let [some-config (merge defaults some-config)]
    ;; (info "Initializing dj-consumer with:")
    ;; (pprint some-config)

    (reset! config (dissoc some-config :db-conn :db-config))
    (db-conn/init some-config)))

(defn remove-!ruby-annotations [s]
  (str/replace s #"!ruby/[^\s]*" ""))

(defn extract-rails-struct-name[s]
  (second (re-find  #"--- *!ruby\/struct:([^\s]*)" s)))

(defn extract-rails-obj-name[s]
  (second (re-find  #"object: *!ruby\/object:([^\s]*)" s)))

(defn parse-ruby-yaml [s]
  (let [struct-name (extract-rails-struct-name s)
        object-name (extract-rails-obj-name s)
        data (yaml/parse-string
              (remove-!ruby-annotations s))
        method-name (:method_name data)
        object-method (str object-name method-name)]
    {:dispatch-key (or (u/camel->keyword struct-name)
                       (if (and object-name method-name) (u/camel->keyword object-name method-name))
                       :default)
     :data data}))

(def job-queue (atom []))

(defn try-job [{:keys [locked-at locked-by failed-at] :as job-record}]
  (try
    :foo
    (catch Exception e :foo))
  )

(defn process-jobs [jobs]

  )


(defn get-all-handlers []
  (->> (db/sql :get-cols-from-table {:table :delayed-job :cols []
                                     :where-clause (cl/conds->sqlvec :delayed-job "" nil [:id] [:id := 2])
                                    })
       (map (fn [{:keys [handler] :as record}] (assoc record :handler (parse-ruby-yaml handler))))))

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
  (init {:sql-log true
         :db-config {:user "root"
                     :password ""
                     :url "//localhost:3306/"
                     :db-name "chin_minimal"
                     ;; :db-name "chinchilla_development"
                     }

         })

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
  (job/perform :user/say-hello {:id 1} nil nil)
  (job/after :user/say-hello {:id 1})
  ;; (pprint handler-str)
  ;; (pprint (parse-ruby-yaml handler-str))
  ;; (pprint (get-all-handlers))
  )
