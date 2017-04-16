(set-env!
 :resource-paths #{"src" "test"}
 :dependencies '[
                 [io.forward/yaml "1.0.6"]

                 [org.clojure/core.async "0.3.441"]

                 [clj-time "0.13.0"]

                 ;; Sql queries
                 [mysql/mysql-connector-java "5.1.40"]
                 ;; TODO: maybe use https://funcool.github.io/clojure.jdbc/latest/ ?
                 [org.clojure/java.jdbc "0.6.1"]
                 ;; For db connection pooling
                 ;; TODO: maybe use https://funcool.github.io/clojure.jdbc/latest/#c3p0 ?
                 [clojure.jdbc/clojure.jdbc-c3p0 "0.3.2"]
                 ;; [mysql/mysql-connector-java "6.0.5"] ;breaks code
                 ;; https://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22org.clojure%22%20AND%20a%3A%22java.jdbc%22
                 ;; [org.clojure/java.jdbc "0.3.7"]
                 ;; [org.clojure/java.jdbc "0.5.8"]

                 ;; [org.postgresql/postgresql "9.4-1212-jdbc41"]
                 ;; [postgresql "9.3-1102.jdbc41"]
                 [com.layerware/hugsql "0.4.7"]
                 ;; [honeysql "0.7.0"]

                 [yesql "0.5.3"]

                 ;; String manipulation
                 [funcool/cuerdas "2.0.3"]

                 ;; https://github.com/samestep/boot-refresh
                 ;; Task to reload clojure code automatically on save
                 [samestep/boot-refresh "0.1.0" :scope "test"]
                 [adzerk/boot-test "1.2.0" :scope "test"]

                 ;; Logging
                 [com.taoensso/timbre      "4.8.0"]
                 ;; [com.taoensso/encore      "2.90.1"]
                 [jansi-clj "0.1.0"]
                 ]
 )

(require
 '[samestep.boot-refresh :refer [refresh]]
  '[adzerk.boot-test :refer :all]
 )

(task-options!
 pom {:project 'dj-consumer
       :version "0.1.0"})


(deftask watch-and-install []
  (comp
   (repl :server true)
   (refresh)
   (watch)
   (pom)
   (jar)
   (install)))

(deftask install-local []
  (comp
   (pom)
   (jar)
   (install)))
