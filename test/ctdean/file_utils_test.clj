;;;;
;;;; file_utils_test
;;;;
;;;; Chris Dean

(ns ctdean.file-utils-test
  (:use ctdean.file-utils)
  (:use (clojure.contrib test-is)))

;; Just an handfule of simple tests for now
(deftest test-file-utils
  (is (= (file-basename "/foo/bar.clj") "bar.clj"))
  (is (= (file-basename "bar.clj") "bar.clj"))
  (is (> (count (file-abs-path "foo.clj")) (count "foo.clj")))
  (is (> (count (file-path "~/bar/foo.clj")) (count "~/bar/foo.clj")))
  (is (= (file-extension "foo/bar.html") ".html"))
  (is (not (file-exists? (str "/some/missing/dir/" (gensym))))))

