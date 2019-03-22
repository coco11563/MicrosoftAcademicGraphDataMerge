name := "MicrosoftAcademicGraphDataMerge"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided"

//libraryDependencies +=  "redis.clients" % "jedis" % "3.0.0"
libraryDependencies += "com.redislabs" % "spark-redis" % "2.3.1-M1"