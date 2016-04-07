name := "calcite-parquet"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.calcite" % "calcite-core" % "1.7.0"
libraryDependencies += "ch.qos.logback" %  "logback-classic" % "1.1.6"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
libraryDependencies += "org.apache.parquet" % "parquet-column" % "1.8.1"
libraryDependencies += "org.apache.parquet" % "parquet-hadoop" % "1.8.1"
libraryDependencies += "org.apache.parquet" % "parquet-format" % "2.3.1"
libraryDependencies += "org.apache.parquet" % "parquet-tools" % "1.8.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.2"

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"