(ns dj-consumer.database.queries
  (:require
   [hugsql.core :as hugsql]))

(def sql-file "dj_consumer/database/hugsql.sql" )

;; Load all sql fns into this namespace
(hugsql/def-db-fns sql-file  {:quoting :mysql})
(hugsql/def-sqlvec-fns sql-file ;; {:quoting :mysql}
  )
