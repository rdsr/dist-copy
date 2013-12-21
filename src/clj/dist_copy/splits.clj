(ns dist-copy.splits
  (:require [clojure.string :as s])
  (:import [dist_copy.io Split Chunk Block]
           [java.io IOException]
           [java.util HashMap HashSet LinkedList Iterator]
           [org.apache.hadoop.io NullWritable SequenceFile]
           [org.apache.hadoop.net NodeBase]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path PathFilter LocatedFileStatus RemoteIterator]
           [org.apache.hadoop.util StringUtils]))


(defn- input-paths
  "Returns, as a list, the user specified input paths.
Paths are comma separated, could contain files and directories,
path can also be glob patterns.
i.e: '/tmp/f*.tgz, /tmp/dir2'"
  [conf]
  (let [dirs (.get conf "dist.copy.input.paths")]
    (for [dir (s/split dirs #", *")]
      (-> dir StringUtils/unEscapeString Path.))))



(defn- glob-status-for-path
  "Returns all paths which match the glob
pattern and satisfy the path filter."
  [conf path path-filter]
  (let [fs (.getFileSystem path conf)
        r (.globStatus fs path path-filter)]
    (if (nil? r)
      (throw (IOException. (str "Input path: " path " does not exist.")))
      r)))


(defn- glob-status
  "Returns all paths which match the glob-patterns
and also satisfy the path filter"
  [conf paths path-filter]
  (mapcat #(glob-status-for-path conf % path-filter) paths))


(defn- path-ancestors
  "Given a path returns all it's ancestors including
the path itself /a/b/c --> /a/b/c /a/b /a /"
  [path]
  (take-while (complement nil?)
              (iterate #(.getParent %) path)))


(defn- remove-redundant-files
  "Removes redundant files from the input so that we
don't needlessly process them again. e.g. If the
input contains /tmp and /tmp/a1, /tmp/a1 is removed
from the result"
  [conf file-statuses]
  (mapcat (fn [path]
            (-> path
              (.getFileSystem conf)
              (.listStatus path)))
          (reduce (fn [hs path]
                    (if (some hs (path-ancestors path))
                      hs
                      (conj hs path)))
                  #{}
                  (sort (map #(.getPath %) file-statuses)))))


(defn- remote-iterator-seq
  "Returns a seq on the remote file-status iterator.
The remote iterator is received when we list the
contents of an hdfs directory"
  [remote-iter]
  (lazy-seq
    (if (.hasNext remote-iter)
      (cons (.next remote-iter) (remote-iterator-seq remote-iter))
      nil)))


(defn- remote-files-seq
  "Lists all files under the specified hdfs directoy"
  [conf dir-status]
  (let [path (.getPath dir-status)
        fs (.getFileSystem path conf)
        remote-iter (.listLocatedStatus fs path)]
    (remote-iterator-seq remote-iter)))


(defn- list-status-recursively
  "Recursively list all files under the dirs specified"
  [conf file-statuses]
  (mapcat (fn [file-status]
            (if (.isDirectory file-status)
              (list-status-recursively
                conf
                (remote-files-seq conf file-status))
              [file-status]))
          file-statuses))


(defn- list-status
  "Recursively list all files under 'dist.copy.input.paths'. If
the input contains files, those files are also listed. Also removes
duplicate files from further processing."
  [conf]
  (let [paths (input-paths conf)
        hidden-files-filter
        (reify PathFilter
          (accept [_ p]
            (let [name (.getName p)]
              (not (or (.startsWith name "_") (.startsWith name "."))))))]
    (list-status-recursively
      conf
      (remove-redundant-files
        conf
        (glob-status
          conf paths hidden-files-filter)))))


(defn- file-blocks
  "Returns all blocks, as a map {:o offset, :p path,
:l length, :h (hosts) :r (racks)}, for a given file"
  [conf file-status]
  (let [path (.getPath file-status)
        fs   (.getFileSystem path conf)]
  (letfn [(racks [_b] 
                 (map (fn [tp]
                        (-> tp NodeBase. .getNetworkLocation))
                      (.getTopologyPaths _b)))
          (block [_b]
                 {:p path
                  :o (.getOffset _b)
                  :l (.getLength _b)
                  :h (-> _b .getHosts seq)
                  :r (racks _b)})]
      (map block
           (if (instance? LocatedFileStatus file-status)
             (.getBlockLocations file-status)
             (.getFileBlockLocations 
               fs file-status 0 (.getLen file-status)))))))
               

(defn- all-blocks
  "Returns all blocks for each input file specified"
  [conf]
  (mapcat (partial file-blocks conf) (list-status conf)))


(defn- compute-split-size
  [conf data-size]
  (let [default-block-size (* 128 1024 1024)]
    (max (.getInt conf "dist.copy.min.split.size" default-block-size)
         (/ data-size (.getInt conf "dist.copy.num.tasks" 1)))))


(defn- total-size
  [blocks]
  (reduce
    (fn [sum b]
      (+ sum (:l b))) 0 blocks))


;; higly imperative code follows :(
;; Todo: maybe try transients on my nested maps here

(defn- group-blocks-by
  ([f blocks]
    (group-blocks-by (HashMap.) f blocks))
  ([m f blocks]
    (letfn [(group-fn 
              [block]
              (let [r (f block)]
                (if (coll? r) r [r])))]
      (doseq [b blocks k (group-fn b)]
        (when (not (contains? m k))
          (.put m k (LinkedList.)))
        (.put m k (doto (get m k) (.offer b))))
      m)))


(defn- blocks->chunks
  "Converts a coll of blocks, grouped by host or rack, to a coll of @dist_copy.io.Chunks"
  [conf grouped-by blocks]
  (let [block (first blocks)
        [host rack] (if (some #(= % grouped-by) (:h block))
                      [grouped-by nil]
                      [nil grouped-by])]
    (for [[path grouped-blocks] 
          (group-blocks-by :p blocks)] 
      ;; TODO: grouped-blocks can contain consecutive blocks, optimise this later
      (Chunk. path
              host 
              rack
              (map (fn [{:keys [o l h r]}]
                     (Block. o l h r))
                   grouped-blocks)))))



(defn- create-split-fn
  [conf seqfile-writer used-blocks]
  (fn [grouped-by blocks]
    (.addAll used-blocks blocks)
    (.append seqfile-writer (NullWritable/get) (Split. (blocks->chunks conf grouped-by blocks)))))


(defn- create-splits
  [k-blocks ks create-split enough-blocks not-enough-blocks]
  (while (not (empty? k-blocks))
    (let [k (rand-nth ks)
          [enough? blocks] (enough-blocks (get k-blocks k))]
      (if enough? 
        (create-split k blocks) 
        (do 
          (.remove k-blocks k) 
          (not-enough-blocks k blocks))))))


(defn- enough-blocks
  [split-size used-blocks blocks]
  (loop [sz 0 acc (LinkedList.)]
    (cond
      (>= sz split-size) [true acc]
      (empty? blocks) [false acc]
      :else (let [b (.poll blocks)]
              (if (contains? used-blocks b)
                  (recur sz acc)
                  (recur (+ sz (:l b)) (doto acc (.offer b))))))))


(defn- splits-file-path
  [conf]
  (Path.
    ; TODO: check this, it may not be incorrect
    (str (.get conf "yarn.app.mapreduce.am.staging-dir")
         "/" (.get conf "yarn.app.attempt.id") "/splits.info.seq")))


(defn- seqfile-writer
  [conf]
  (let [path (splits-file-path conf)
        fs (.getFileSystem path conf)]
    (SequenceFile/createWriter
      fs conf path NullWritable Split)))


(defn create-splits-file
  [conf]
  (let [blocks      (all-blocks conf)
        host-blocks (group-blocks-by :h blocks)
        split-size  (compute-split-size conf (total-size blocks))]

    (with-open [sf-wr (seqfile-writer conf)]
      (let [used-blocks   (HashSet.)
            rack-blocks   (HashMap.)
            create-split  (create-split-fn conf sf-wr used-blocks)
            enough-blocks (partial enough-blocks split-size used-blocks)
            not-enough-blocks (fn [host-or-rack blocks] 
                                (group-blocks-by rack-blocks :r blocks))]

        (create-splits
          host-blocks (vec (keys host-blocks)) 
          create-split enough-blocks not-enough-blocks)
        (create-splits 
          rack-blocks (vec (keys rack-blocks)) 
          create-split enough-blocks create-split)))))

(def conf (Configuration.))
(.set conf "dist.copy.input.paths" "/tmp/f1, /tmp/hadoop*gz*")
(.set conf "yarn.app.attempt.id" (str (rand-int 200)))
(create-splits-file conf)