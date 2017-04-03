(ns dj-consumer.core
  (:require
   [dj-consumer.database :as db]
   [dj-consumer.config :refer [config]]
   [dj-consumer.util :as u]

   [jdbc.pool.c3p0 :as pool]
   [yaml.core :as yaml]

   ;; String manipulation
   [cuerdas.core :as str]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]))

(defn init [{:keys [db-conn db-config] :as some-config}]
  (info "Initializing dj-consumer with:")
  (pprint some-config)
  (reset! config (dissoc some-config :db-conn :db-config))
  (db/init some-config))

;; (defn test-db []
;;   (db/))


(init {:db-config {:user "root"
                   :password ""
                   :url "//localhost:3306/"
                   :db-name "chin_minimal"
                   }})

(pprint (db/sql :select-all-from {:table :delayed-job}))

(yaml/generate-string {:foo :bar})
