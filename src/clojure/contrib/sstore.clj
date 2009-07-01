;;;;
;;;; sstore - Serialize a data structure as clojure readable strings. 
;;;;
;;;; Chris Dean

(ns clojure.contrib.sstore)

(defn to-sstore [obj]
  (binding [*print-dup* true]
    (with-out-str 
      (pr obj))))

(defn from-sstore [sstore]
  (read-string sstore))
