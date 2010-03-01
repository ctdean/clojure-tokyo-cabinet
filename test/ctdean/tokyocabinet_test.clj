;;;;
;;;; tokyocabinet_test
;;;;
;;;; Chris Dean

(ns ctdean.tokyocabinet-test
  (:use ctdean.tokyocabinet)
  (:use ctdean.file-utils)
  (:use (clojure.contrib test-is)))

;;;
;;; Tests
;;;

(defn test-simple-db [db]
  (db-add db :a 1)
  (db-add db :b 2)
  (db-add db :c 3)
  (is (contains? db :a))
  (is (not (contains? db :x)))
  (is (= 2 (get db :b)))
  (is (= 3 (count (seq db)) (count db)))
  (db-add db :b {"yes" 88})
  (is (= {"yes" 88} (:b db)))
  (is (nil? (:none db)))
  (is (= 3 (count (seq db)) (count db)))
  (db-add db "<d>" 3.14)
  (is (= 4 (count (seq db)) (count db)))
  (is (= '(["<d>" 3.14] [:a 1] [:b {"yes" 88}] [:c 3])
         (sort #(compare (str %1) (str %2)) (seq db))))
  (db-remove db "<d>")
  (is (= '([:a 1] [:b {"yes" 88}] [:c 3])
         (sort #(compare (str %1) (str %2)) (seq db)))))

(defn test-dup-db [db]
  (db-add-dup db :a "no")
  (is (= '(1 "no") (db-get-dup db :a)))
  (is (= '([:a "no"] [:a 1] [:b {"yes" 88}] [:c 3])
         (sort #(compare (str %1) (str %2)) (seq db)))))

(deftest test-bdb
  (let [fname (str "casket-" (gensym) ".db")]
    (with-open [db (bdb-open fname)]
      (test-simple-db db)
      (test-dup-db db))
    (file-rm fname)))

;;; (deftest test-remote
;;;  (with-open [db (db-remote-open "localhost")]
;;;    (test-simple-db db)))
