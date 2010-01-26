;;;;
;;;; file_utils - the barest sketch of some file utility functions.
;;;;
;;;; Chris Dean

(ns ctdean.file-utils
  (:use (clojure.contrib duck-streams java-utils))
  (:import [java.io File]))

;;;
;;; File utils
;;;

(defmulti file-expand type)
(defmethod file-expand String [s]
  (file-str s))
(defmethod file-expand java.io.File [f] 
  f)

(defn file-basename [file]
  (.getName (file-expand file)))

(defn file-abs-path [file]
  (.getAbsolutePath (file-expand file)))

(defn file-path [file]
  (.getPath (file-expand file)))

(defn file-extension [file]
  (let [basename (file-basename file)
        index (.lastIndexOf basename (int \.))]
    (when-not (neg? index)
      (subs basename index))))

(defn file-exists? [file]
  (.isFile (file-expand file)))

(defn dir-exists? [dir]
  (.isDirectory (file-expand dir)))

(defn file-mv [src dest]
  (.renameTo (file-expand src) (file-expand dest)))

(defn file-rm [file]
  (.delete (file-expand file)))
