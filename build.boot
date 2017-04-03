(set-env!
 :resource-paths #{"src"}
 :dependencies '[

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

                 ;; Logging
                 [com.taoensso/timbre      "4.8.0"]
                 ;; [com.taoensso/encore      "2.90.1"]
                 ]
 )

(task-options!
 pom {:project 'dj-consumer
       :version "0.1.0"}
 jar {:manifest {"Foo" "bar"}})


(deftask watch-and-install []
  (comp
   (watch)
   (pom)
   (jar)
   (install)))
