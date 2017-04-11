(ns dj-consumer.database.clauses
  (:require
   [dj-consumer.database.queries :refer [clause-snip cond-snip where-snip limit-snip]]

   ;; String manipulation
   [cuerdas.core :as str]

   [dj-consumer.util :as u :refer [hyphen->underscore parse-natural-number includes?]]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]

   ;; debug
   [clojure.pprint :refer (pprint)]
   ;; https://github.com/xsc/jansi-clj
   ;; (println "ERROR:" (underline "This" " is " "a message."))
   [jansi-clj.core :refer :all]))

(defn get-p-type [p]
  (cond (or (= p :null) (= p :NULL)) :n
        (keyword? p) :i
        (or (sequential? p) (set? p)) :v*
        :else :v))

(def operators [:= :<> :> :< :>= :<= :LIKE :IN :like :in :is :IS :is-not :IS-NOT])

(defn assert-value [p]
  (if-not (or (string? p) (number? p))
    (throw (ex-info
            (str p " should be a string or number") {}))))

(defn assert-col [c cols]
  (if-not (or (nil? cols) (includes? cols c))
    (throw (ex-info
            (str c " is not an allowed column for this table") {:col c :cols cols}))))

(defn assert-op [op]
  (if-not (or (= op :in) (= op :IN))
    (throw (ex-info
            (str "only :in operator is allowed when second value is a list or vector")
            {:op op}))))

(defn assert-is [op]
  (if-not (includes? [:is :is-not :IS-NOT :IS] op)
    (throw (ex-info
            (str "only :is or :is-not operator is allowed when second value is a :null")
            {:op op}))))

(defn lookup-prop
  "If p is namespaced at all, then return value of name of p in
  props"
  [props p]
  (let [prop? (and (keyword? p) (namespace p))]
    (if prop?
      (let [prop-keyword (keyword (name p))]
        (get props prop-keyword)))))

(defn cond->cond-snip-params [env t alias-prefix props cols [p1 op p2]]
  (if-not (includes? operators op)
    (throw (ex-info  (str op " is not an operator") {})))
  (let [p1 (or (lookup-prop props p1) p1)
        p2 (or (lookup-prop props p2) p2)
        found-cols []
        p1-type (get-p-type p1)
        found-cols (if (= p1-type :i) (conj found-cols p1) [])
        p2-type (get-p-type p2)
        found-cols (if (= p2-type :i) (conj found-cols p2) found-cols)
        p-types (keyword (str (name p1-type) (name p2-type)))
        ]
    (if (= p1-type :v) (assert-value p1))
    (if (and cols (= p1-type :i)) (assert-col p1 cols))
    (if (= p2-type :v) (assert-value p2))
    (if (and cols (= p2-type :i)) (assert-col p2 cols))
    (if (= p-types :iv*) (assert-op op) )
    (if (= p-types :in) (assert-is op) )
    (if-not (includes? [:iv :iv* :vi :ii :vv :in] p-types)
      (throw (ex-info
              (str "comparison is not correct for " p1 " and " p2)
              {:cond [p1 op p2]})))
    (let [prefix-table-name (fn [p] (str alias-prefix (if (str/contains? p ".") p (str (u/table-name env t) "." p))))
          p1 (if (= p1-type :i) ((comp prefix-table-name hyphen->underscore name) p1) p1)
          p2 (if (= p2-type :i) ((comp prefix-table-name hyphen->underscore name) p2) p2)
          p2 (if (= p2-type :n) "NULL" p2)
          op (str/replace (name op) "-" " ")]
      {:cond [p1 op p2] :p-types p-types :found-cols found-cols})))

;; (cond->cond-snip-params :t {} [:a] [:a :is 1])
;; (cond->cond-snip-params :t {:a [1 2] :b 2} [:c.b1 :c2] [:c.b1 :in :p/a])

(defn clause? [[k v]]
  (and (keyword? k) (vector? v)))

;; TODO: memoize!!
(defn conds->sqlvec
  "Takes a list of valid columns and a vector such as [:or [[:a :<
  1] [:and [[:b :in [1 2]] [:c :< 3]]]]] and returns a sqlvec to be
  passed to a hugsql db query as the where param. Pass nil for cols to
  return vector of column keys uses in conds."
  [env t alias-prefix props cols conds]
   (let [found-cols (atom [])]
     (letfn [(make-sqlvec [prefix conds]
               (if (clause? conds)
                 (let [logical-operator (first conds)
                       conds (second conds)
                       n-conds (count conds)]
                   (if-not (includes? [:or :and :OR :AND] logical-operator)
                     (throw (ex-info
                             (str "unknown logical operator " logical-operator) {})))
                   (if-not (pos? n-conds)
                     (throw (ex-info  (str "empty " logical-operator " clause") {})))
                   (condp = n-conds
                     1 (make-sqlvec prefix (first conds))
                     (let [logical-operator (str/upper (name logical-operator))
                           first-cond (make-sqlvec nil (first conds))
                           rest-conds (map (partial make-sqlvec logical-operator) (rest conds)) ;recurse
                           conds (cons first-cond rest-conds)
                           params {:cond conds}
                           params (if prefix (assoc params :prefix prefix) params)]
                       (clause-snip params)))) ;make clause snip
                 (let [params (cond->cond-snip-params env t alias-prefix props cols conds)
                       params (if prefix (assoc params :prefix prefix) params)]
                   (swap! found-cols concat (:found-cols params))
                   (cond-snip params))))] ;make cond snip
       (let [result (where-snip {:clause (make-sqlvec nil conds)})]
         (if cols
           result
           (vec @found-cols))))))

;; (cond-snip  {:p-types :in :cond ["a" "is" "null"]})

;; (conds->sqlvec :t {} [:a] [:a :is :null])
;; (conds->sqlvec {:g 1} [:a-b :b-d :c] [:and [ [:a-b := :b-d]]])
(defn order-by->sqlvec
  "Takes a list of valid columns and returns a order-by sqlvec to be
  passed to a hugsql query as the order-by param"
  [env t alias-prefix cols order-by]
  [(str "order by "
        (str/join ", " (map (fn [[col dir]]
                              (if (nil? col) (throw (ex-info "Can't order by nil column :-)" {:table t
                                                                                              :cols cols
                                                                                              :order-by order-by})))
                              (assert-col col cols)
                              (if-not (includes? [nil :desc :asc :DESC :ASC] dir)
                                (throw (ex-info (str dir " is not a valid order direction") {:table t
                                                                                             :cols cols
                                                                                             :order-by order-by})))
                              (str alias-prefix (u/table-name env t)  "."
                                   (hyphen->underscore (name col))
                                   (if dir (str " " (name dir)))))
                            order-by)))])

;; (order-by->sqlvec {} :job-table "" [:c1] [[:c1]])

(defn make-where-clause [env t alias-prefix scope cond {:keys [where]} props cols]
  (if (or scope cond where)
    (let [conds (filterv some? [scope cond where])]
      ;If there's only one cond in the and, the and will be removed by conds->sqlvec
      (conds->sqlvec env t alias-prefix props cols [:and conds]))))

(defn make-order-by-clause [env t alias-prefix {:keys [order-by]} cols]
  (if (and order-by (pos? (count order-by)))
    (order-by->sqlvec env t alias-prefix cols order-by)))

(defn make-limit-clause [env {:keys [count]}]
  (limit-snip {:count count}))
