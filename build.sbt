import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.8",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "KPE-Spark"
  )

//initialize := {
//  val _ = initialize.value // run the previous initialization
//  val required = "1.8"
//  val current  = sys.props("java.specification.version")
//  assert(current == required, s"Unsupported JDK: java.specification.version $current != $required")
//}

/*
//unmanagedJars in Compile += file("lib/kpe-1.0.25.jar")
*/

libraryDependencies += "com.atilika.kuromoji" % "kuromoji-ipadic" % "0.9.0"
// Change this to another test framework if you prefer
//libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-common" % "2.8.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-client" % "2.8.0"

//XML support
//libraryDependencies += "com.databricks" % "spark-xml_2.11" % "0.4.1"


addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

//libraryDependencies += "io.localKanji" %% "sjt" % "1.0"
libraryDependencies += "io.localKanji" %% "sjt" % "latest.[any status]"