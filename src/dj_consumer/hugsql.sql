-- :name select-all-from
SELECT * from :i:table

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
