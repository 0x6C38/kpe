import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.8",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "KPE-Spark"
  )
/*
val circeVersion = "0.8.0"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies += "io.iteratee" % "iteratee-core_2.11" % "0.12.0"
libraryDependencies += "io.iteratee" %% "iteratee-scalaz" % "0.12.0"
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.14"
libraryDependencies += "org.scalaz" %% "scalaz-iteratee" % "7.2.14"
libraryDependencies += "org.scalaz" %% "scalaz-concurrent" % "7.2.14"

//unmanagedJars in Compile += file("lib/kpe-1.0.25.jar")
*/
// Change this to another test framework if you prefer
libraryDependencies += "com.atilika.kuromoji" % "kuromoji-ipadic" % "0.9.0"
//libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-common" % "2.8.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-client" % "2.8.0"


addCompilerPlugin(
  "org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full
)

libraryDependencies += "io.localKanji" %% "sjt" % "1.0"
