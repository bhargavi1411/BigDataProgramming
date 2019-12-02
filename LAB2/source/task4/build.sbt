name := "untitled5"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.3.2"

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"
