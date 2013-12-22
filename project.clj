(defproject dist-copy "0.1.0-SNAPSHOT"
  :source-paths ["src/clj"]
  :test-paths  ["test/clj"]
  :java-source-paths ["src/jvm"]

  :resource-paths ["conf"]
  :description "Distributed copy using YARN"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [reply "0.2.1"]
                 [org.apache.hadoop/hadoop-common "0.23.9"]
                 [org.apache.hadoop/hadoop-hdfs "0.23.9"]
                 [org.apache.hadoop/hadoop-hdfs "0.23.9" :scope "test" :classifier "tests"]
                 [org.apache.hadoop/hadoop-mapreduce-client-core "0.23.9"]
                 [org.apache.hadoop/hadoop-mapreduce-client-common "0.23.9"]
                 [org.apache.hadoop/hadoop-yarn-common "0.23.9"]])
