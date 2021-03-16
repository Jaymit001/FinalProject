name := "Final"

version := "0.1"

scalaVersion := "2.11.8"

val hadoopVersion = "2.7.7"

val zookeeperVersion = "3.4.14"

val sparkVersion ="2.4.4"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % sparkVersion,

  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion)


libraryDependencies += "org.apache.curator" % "curator-framework" % "2.8.0"

libraryDependencies += "org.apache.curator" % "curator-recipes" % "2.8.0"


libraryDependencies += "org.apache.hive" %"hive-jdbc" % "1.1.0-cdh5.16.2"

resolvers += "Cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies += "org.apache.kafka" %"kafka-clients"%"2.3.1"
libraryDependencies += "org.apache.zookeeper" % "zookeeper" % zookeeperVersion

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion

val ConfluentVersion = "5.3.0"
resolvers += "Confluent" at "https://packages.confluent.io/maven/"
libraryDependencies += "io.confluent" % "kafka-schema-registry-client" % ConfluentVersion
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % ConfluentVersion
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.5"
