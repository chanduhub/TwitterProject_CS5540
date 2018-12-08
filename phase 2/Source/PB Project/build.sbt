name := "BDP Module2"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.3.2"

// https://mvnrepository.com/artifact/graphframes/graphframes
libraryDependencies += "graphframes" % "graphframes" % "0.6.0-spark2.3-s_2.11"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.0" % "runtime"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0" % "runtime"

