(ns ctdean.tcsample
  (:use clojure.contrib.pprint)
  (:use ctdean.tokyocabinet))

(def *db* (bdb-open "/tmp/foo.db"))

(defn add-sample-data []
  (db-clear *db*)
  (db-add *db* :a 1)
  (db-add *db* :b 2)
  (db-add *db* :c 3))

(defn print-sample-data []
  (print "*db* => ")
  (pprint (seq *db*))
  (print "(:b *db*) => ")
  (pprint (:b *db*))
  (print "(:missing *db*) => ")
  (pprint (:missing *db*)))

