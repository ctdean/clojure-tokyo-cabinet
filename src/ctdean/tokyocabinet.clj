;;;;
;;;; tokyocabinet - interface to the Tokyo Cabinet database.  We only
;;;; support the B+ Tree Database.
;;;;
;;;; Chris Dean

(ns ctdean.tokyocabinet
  (:use (clojure.contrib def except test-is java-utils))
  (:use (ctdean file-utils sstore))
  (:import [tokyocabinet BDB BDBCUR]
           [tokyotyrant RDB])
  (:import [clojure.lang Counted Associative IFn Seqable]))
  
(defvar *READ-WRITE* (bit-or BDB/OWRITER BDB/OCREAT)
  "Open (or create) for reading and writing")

(defvar *READ-ONLY* BDB/OREADER
  "Open for reading")

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

(defn- bdb-handle-seq [handle]
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

(defn- tt-handle-seq [handle]
  "Return a lazy sequence of all the key value pairs in the
   database."
  (.iterinit handle)
  (let [step (fn step []
               (when-let [key-str (.iternext handle)]
                 (when-let [val-str (.get handle key-str)]
                   (lazy-seq
                     (cons (make-map-entry (from-sstore key-str) 
                                           (from-sstore val-str))
                           (step))))))]
    (step)))

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
    (seq [] (bdb-handle-seq handle))))

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

(defmulti db-error-message (fn [x] (type (x))))
(defmethod db-error-message tokyocabinet.BDB [db]
  (.errmsg (db)))
(defmethod db-error-message tokyotyrant.RDB [db]
  "error")

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
     
(defn make-tt-proxy [handle]
  "Create a proxy object from the Tokyo Tyrant handle object.

   Supports the count, seq, and map lookup (get et.al).  Calling this
   object as a function with no arguments returns the handle.

   Remember that the database is mutable, unlike most clojure data
   structures."
  (proxy [Counted Associative IFn Seqable java.io.Closeable] []
    (count [] (.rnum handle))
    (toString [] (str (get (.stat handle) "path" "?") " " handle))
    (containsKey [key] (not= -1 (.vsiz handle (to-sstore key))))
    (entryAt [key] (when-let [val-str (.get handle (to-sstore key))]
                     (make-map-entry key (from-sstore val-str))))
    (valAt ([key] (lookup-key handle key nil))
           ([key not-found] (lookup-key handle key not-found)))
    (invoke ([key] (lookup-key handle key nil))
            ([key not-found] (lookup-key handle key not-found))
            ([] handle))                ; Silly hack, I feel so ashamed.
    (close [] (.close handle))
    (seq [] (tt-handle-seq handle))))

(defn db-remote-open 
  "Open a Tokyo Tyrant connection"
  ([host] (db-remote-open host 1978))
  ([host port] (db-remote-open host port 0))
  ([host port timeout-ms] 
     (let [handle (new RDB)]
       (.open handle (new java.net.InetSocketAddress host port) timeout-ms)
       (make-tt-proxy handle))))

(defn db-close [db]
  "Close a Tokyo Cabinet B+ Tree database"
  (when-not (.close (db))
    (throwf "Unable to close: %s" (db-error-message db))))

(defmacro- ignore-errors [expr]
  `(try
    (do ~expr)
    (catch Throwable e#
      nil)))

(defmulti db-valid?
    "Is this a valid db object?"
    (fn [x] (if x
                (type (x))
                :default)))

(defmethod db-valid? :default [db]
  nil)

(defmethod db-valid? tokyocabinet.BDB [db]
  (ignore-errors (.path (db))))

(defmethod db-valid? tokyotyrant.RDB [db]
  (ignore-errors (.stat (db))))

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
