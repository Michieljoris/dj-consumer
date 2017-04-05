(ns dj-consumer.database.info
  (:require
   [dj-consumer.util :as u]
   )  )


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
