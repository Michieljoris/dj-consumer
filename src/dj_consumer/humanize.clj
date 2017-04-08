(ns dj-consumer.humanize)

;;Relevant parts copied from https://github.com/trhura/clojure-humanize
;;No need for dependency

(defn pluralize-noun [count noun]
  "Return the pluralized noun if the `count' is
   not 1."
  {:pre [(<= 0 count)]}
  (let [singular? (== count 1)]
    (if singular?
      noun                                                  ; If singular, return noun
      (str noun "s"))))

(defn numberword [n]
  (str n))

(def ^:private duration-periods
  [[(* 1000 60 60 24 365) "year"]
   [(* 1000 60 60 24 31) "month"]
   [(* 1000 60 60 24 7) "week"]
   [(* 1000 60 60 24) "day"]
   [(* 1000 60 60) "hour"]
   [(* 1000 60) "minute"]
   [1000 "second"]])

(defn- duration-terms
  "Converts a duration, in milliseconds, to a set of terms describing the duration.
  The terms are in descending order, largest period to smallest.
  Each term is a tuple of count and period name, e.g., `[5 \"second\"]`.
  After seconds are accounted for, remaining milliseconds are ignored."
  [duration-ms]
  {:pre [(<= 0 duration-ms)]}
  (loop [remainder duration-ms
         [[period-ms period-name] & more-periods] duration-periods
         terms []]
    (cond
      (nil? period-ms)
      terms

      (< remainder period-ms)
      (recur remainder more-periods terms)

      :else
      (let [period-count (int (/ remainder period-ms))
            next-remainder (mod remainder period-ms)]
        (recur next-remainder more-periods
               (conj terms [period-count period-name]))))))

(defn duration
  "Converts duration, in milliseconds, into a string describing it in terms
  of years, months, weeks, days, hours, minutes, and seconds.
  Ex:
     (duration 325100) => \"five minutes, twenty-five seconds\"
  The months and years periods are not based on actual calendar, so are approximate; this
  function works best for shorter periods of time.
  The optional options map allow some control over the result.
  :list-format (default: a function) can be set to a function such as oxford
  :number-format (default: numberword) function used to format period counts "
  {:added "0.2.1"}
  ([duration-ms]
   (duration duration-ms nil))
  ([duration-ms options]
   (let [terms (duration-terms duration-ms)
         {:keys [number-format list-format short-text]
          :or {number-format numberword
               ;;MODIFIED: Also print ms durations
               short-text (str duration-ms "ms")
               ;; This default, instead of oxford, because the entire string is a single "value"
               list-format #(clojure.string/join ", " %)}} options]
     (if (seq terms)
       (->> terms
            (map (fn [[period-count period-name]]
                   (str (number-format period-count)
                        " "
                        (pluralize-noun period-count period-name))))
            list-format)
short-text))))
