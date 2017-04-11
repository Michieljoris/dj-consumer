-- :name select-all-from
SELECT * from :i:table

-- :name inc-attempts :! :n
update :i:table
  set attempts = attempts + 1
  where id = :id

-- :name delete-record :! :n
delete from :i:table where id = :id

-- :name update-record :! :n
/* :require [clojure.string :as string]
            [hugsql.parameters :refer [identifier-param-quote]] */
update :i:table set
/*~
(string/join ","
  (for [[field _] (:updates params)]
    (str (identifier-param-quote (name field) options)
      " = :v:updates." (name field))))
~*/
--~ (when (:where-clause params) ":snip:where-clause")
--~ (when (:order-by-clause params) ":snip:order-by-clause")
--~ (when (:limit-clause params) ":snip:limit-clause")


-- :name get-cols-from-table :? :*
select
--~ (if (seq (:cols params)) ":i*:cols" "*")
from :i:table
--~ (when (:where-clause params) ":snip:where-clause")
--~ (when (:order-by-clause params) ":snip:order-by-clause")
--~ (when (:limit-clause params) ":snip:limit-clause")

--- ***************  where clause
-- :snip where-snip
where :snip:clause

-- :snip clause-snip
--~ (:prefix params)
(:snip*:cond)

-- :snip cond-snip
--~ (or (:prefix params) )
/*~
(condp = (:p-types params)
:iv ":i:cond.0 :sql:cond.1 :v:cond.2"
:iv* ":i:cond.0 :sql:cond.1 (:v*:cond.2)"
:vi ":v:cond.0 :sql:cond.1 :i:cond.2"
:ii ":i:cond.0 :sql:cond.1 :i:cond.2"
:vv ":v:cond.0 :sql:cond.1 :v:cond.2"
:in ":i:cond.0 :sql:cond.1 NULL"
"")
~*/

--- *************** order clause
-- :snip order-snip
order by :snip*:cols

-- :snip order-col-snip
--~ (str ":i:col" (when (:dir params) " :sql:dir"))

--- *************** limit clause
-- :snip limit-snip
limit :count
--~ (when (:offset params) "offset :offset")

-- :name now
select now();

--- *************** Testing
-- :name drop-db :!
drop database if exists :i:db-name;

-- :name create-db :!
create database if not exists :i:db-name;

-- :name create-table :!
CREATE TABLE :i:table-name (:i*:columns)

-- :name insert-rows :! :n
-- :doc Insert multiple rows with :tuple* parameter type
insert into :i:table-name (:i*:cols)
values :tuple*:rows
