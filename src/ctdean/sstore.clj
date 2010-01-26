;;;;
;;;; sstore - Serialize a data structure as clojure readable strings. 
;;;;
;;;; Chris Dean

(ns ctdean.sstore
  (import [java.io Writer]))

(defmethod print-dup java.util.Date [o, #^Writer w]
  (print-ctor o
              (fn [o w] (print-dup (str o) w))
              w))

(defmethod print-dup java.util.UUID [o, #^Writer w]
  (.write w "#=(")
  (.write w "java.util.UUID/fromString ")
  (print-dup (str o) w)
  (.write w ")"))

(defn to-sstore [obj]
  (binding [*print-dup* true]
    (with-out-str 
      (pr obj))))

(defn from-sstore [sstore]
  (read-string sstore))
