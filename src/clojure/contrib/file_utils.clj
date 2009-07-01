;;;;
;;;; file_utils - the barest sketch of some file utility functions.
;;;;
;;;; Chris Dean

(ns clojure.contrib.file-utils
  (:use clojure.contrib.java-utils)
  (:import [java.io File]))

(defn file-expand-string
  "Returns a file name by replacing all / and \\ with
  File/separatorChar.  Replaces ~ at the start of the path with the
  user.home system property."  
  [#^String s]
  (let [s (.replace s \/ File/separatorChar)
        s (.replace s \\ File/separatorChar)
        s (if (.startsWith s "~")
              (str (System/getProperty "user.home")
                   File/separatorChar (subs s 1))
              s)]
    s))

(defmulti file-expand type)
(defmethod file-expand String [s]
  (new File (file-expand-string s)))
(defmethod file-expand File [f] 
  f)

(defn file-basename [file]
  (.getName (file-expand file)))

(defn file-abs-path [file]
  (.getAbsolutePath (file-expand file)))

(defn file-extension [file]
  (let [basename (file-basename file)
        index (.lastIndexOf basename (int \.))]
    (when-not (neg? index)
      (subs basename index))))

(defn file-exists? [file]
  (.isFile (file-expand file)))

(defn file-rm [file]
  (.delete (file-expand file)))
