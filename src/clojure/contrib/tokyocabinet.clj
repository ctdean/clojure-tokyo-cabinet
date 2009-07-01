;;;;
;;;; tokyocabinet - interface to the Tokyo Cabinet database.  We only
;;;; support the B+ Tree Database.
;;;;
;;;; Chris Dean

(ns clojure.contrib.tokyocabinet
  (:use (clojure.contrib def except sstore test-is java-utils file-utils))
  (:import [tokyocabinet BDB BDBCUR])
  (:import [clojure.lang Counted Associative IFn Seqable]))

(defvar *READ-WRITE* (bit-or BDB/OWRITER BDB/OCREAT)
  "Open (or create) for reading and writing")

(defvar *LARGE-DB* BDB/TLARGE
  "The database can be larger that 2GB.  Use when tuning in the :opts
   parameter.")


(defn- lookup-key [handle key not-found]
  (if-let [val-str (.get handle (to-sstore key))]
    (from-sstore val-str)
    not-found))

(defn- make-map-entry [k v]
  "Create a single database entry object.  Probably should use vector
   instead of this internal clojure class."
  (new clojure.lang.MapEntry k v))

(defn- handle-seq [handle]
  "Return a lazy sequence of all the key value pairs in the
   database."
  (let [cur (new BDBCUR handle)]
    (.first cur)
    (let [step (fn step []
                   (when-let [key-str (.key2 cur)]
                     (when-let [val-str (.val2 cur)]
                       (.next cur)
                       (lazy-seq
                         (cons (make-map-entry (from-sstore key-str) 
                                               (from-sstore val-str))
                               (step))))))]
      (step))))


(defn make-bdb-proxy [handle]
  "Create a proxy object from the Tokyo Cabinet handle object.

   Supports the count, seq, and map lookup (get et.al).  Calling this
   object as a function with no arguments returns the handle.

   Remember that the database is mutable, unlike most clojure data
   structures."
  (proxy [Counted Associative IFn Seqable java.io.Closeable] []
    (count [] (.rnum handle))
    (toString [] (str (.path handle) " " handle))
    (containsKey [key] (not (zero? (.vnum handle (to-sstore key)))))
    (entryAt [key] (when-let [val-str (.get handle (to-sstore key))]
                     (make-map-entry key (from-sstore val-str))))
    (valAt ([key] (lookup-key handle key nil))
           ([key not-found] (lookup-key handle key not-found)))
    (invoke ([key] (lookup-key handle key nil))
            ([key not-found] (lookup-key handle key not-found))
            ([] handle))                ; Silly hack, I feel so ashamed.
    (close [] (.close handle))
    (seq [] (handle-seq handle))))

(defn tune [db options]
  "Tune the performance of the database.  Should be called before the
   db is opened for the first time.  Options is a map of tunable
   parameters."
  (.tune (db)
         (:lmemb options 0)
         (:nmemb options 0)
         (:bnum options 0)
         (:apow options -1)
         (:fpow options -1)
         (:opts options 0)))

(defn db-error-message [db]
  (.errmsg (db)))

(defn bdb-open 
  "Open a Tokyo Cabinet B+ Tree database"
  ([name] (bdb-open name *READ-WRITE*))
  ([name mode] (bdb-open name mode {}))
  ([name mode options]
     (let [db (make-bdb-proxy (new BDB))]
       (when-not (tune db options)
         (throwf "Unable to tune %s: %s" name (db-error-message db)))
       (when-not (.open (db) (file-abs-path name) mode)
         (throwf "Unable to open %s: %s" (file-abs-path name) 
                 (db-error-message db)))
       db)))
     
(defn db-close [db]
  "Close a Tokyo Cabinet B+ Tree database"
  (when-not (.close (db))
    (throwf "Unable to close: %s" (db-error-message db))))

(defn db-add [db key val]
  "Store a record in the database.  Using an existing key will
   overwrite that entry in the db."
  (when-not (.put (db) (to-sstore key) (to-sstore val))
    (throwf "Unable to add %s: %s" key (db-error-message db)))
  val)
        
(defn db-add-dup [db key val]
  "Store a possibly duplicate record in the database.  Does not
   overwrite existing keys."
  (when-not (.putdup (db) (to-sstore key) (to-sstore val))
    (throwf "Unable to add %s: %s" key (db-error-message db)))
  val)
  
(defn db-get-dup [db key]
  "Get all the values for a given key."
  (when-let [vals-str (.getlist (db) (to-sstore key))]
    (map from-sstore vals-str)))

(defn db-flush [db]
  (.sync (db)))

(defn db-remove [db key]
  "Remove key from that db"
  (.out (db) (to-sstore key)))

(defn db-clear [db]
  "Remove all entries from the db"
  (.vanish (db)))

;;;
;;; Tests
;;;

(deftest test-bdb
  (let [fname (str "casket-" (gensym) ".db")]
    (with-open [db (bdb-open fname)]
      (db-add db :a 1)
      (db-add db :b 2)
      (db-add db :c 3)
      (is (= 2 (get db :b)))
      (is (= 3 (count (seq db)) (count db)))
      (db-add db :b {"yes" 88})
      (is (= {"yes" 88} (:b db)))
      (is (nil? (:none db)))
      (is (= 3 (count (seq db)) (count db)))
      (db-add-dup db :a "no")
      (is (= '(1 "no") (db-get-dup db :a)))
      (is (= '([:a "no"] [:a 1] [:b {"yes" 88}] [:c 3])
             (sort #(compare (str %1) (str %2)) (seq db)))))
    (file-rm fname)))
