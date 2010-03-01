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

(defn #^File file-str-2
  "Concatenates args as strings and returns a java.io.File.  Replaces
   all / and \\ with File/separatorChar.  Replaces ~ at the start of
   the path with the user.home system property.

   This is a copy of the unreleased v1.2 in
   clojure.contrib.io/file-str"
  [& args]
  (let [#^String s (apply str args)
        s (.replace s \\ File/separatorChar)
        s (.replace s \/ File/separatorChar)
        s (if (.startsWith s "~")
              (str (System/getProperty "user.home")
                   File/separator (subs s 1))
              s)]
    (File. s)))

(defmulti file-expand type)
(defmethod file-expand String [s]
  (file-str-2 s))
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
