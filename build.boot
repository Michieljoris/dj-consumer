(def +version+ "0.1.1")

(set-env!
 :resource-paths #{"src" "test"}
 :dependencies '[
                 [org.clojure/clojure "1.8.0"]

                 [io.forward/yaml "1.0.6"]

                 [org.clojure/core.async "0.3.441"]

                 [clj-time "0.13.0"]

                 ;;Git dependencies. Run bin/install-git-deps first
                 [dc-util "0.1.3"]
                 [bilby-libs "0.1.6"]

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

                 ;; String manipulation
                 [funcool/cuerdas "2.0.3"]

                 ;; https://github.com/samestep/boot-refresh
                 ;; Task to reload clojure code automatically on save
                 [samestep/boot-refresh "0.1.0" :scope "test"]
                 [adzerk/boot-test "1.2.0" :scope "test"]
                 [clojure-future-spec "1.9.0-alpha15"]
                 [org.clojure/test.check "0.9.0"]

                 ;; Logging
                 [com.taoensso/timbre      "4.8.0"]
                 ;; [com.taoensso/encore      "2.90.1"]
                 [jansi-clj "0.1.0"]
                 [onetom/boot-lein-generate "0.1.3" :scope "test"]
                 ]
 )

(require '[boot.lein])
(boot.lein/generate)

(require
 '[samestep.boot-refresh :refer [refresh]]
 '[adzerk.boot-test :refer :all]
 )

(task-options!
 pom {:project 'dj-consumer
      :version +version+})


(deftask watch-and-install []
  (set-env! :source-paths #(conj % "test"))
  (comp
   ;; (test)
   (repl :server true)
   (refresh) ;;test dir added source-paths so (eftest) in run-all-tests is run
   (watch)
   (pom)
   (jar)
   (install)))

(deftask repl-only []
  (set-env! :source-paths #(conj % "test"))
  (comp
   ;; (test)
   (repl :server true)
   (refresh)
   (wait)))

(deftask install-local []
  (comp
   (pom)
   (jar)
   (install)))
