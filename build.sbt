import AssemblyKeys._

assemblySettings

name := "embedded-knowledge-recommendation"

organization := "org.apache.predictionio"

parallelExecution in Test := false
libraryDependencies += "org.apache.predictionio" % "apache-predictionio-core_2.10" % "0.10.0-incubating"

libraryDependencies ++= Seq(
 "org.apache.predictionio" %% "apache-predictionio-core_2.10" % "0.10.0-incubating",
  "org.apache.spark"        %% "spark-core"               % "1.4.1" % "provided",
"org.apache.spark" %% "spark-mllib" % "1.4.1" % "provided",
"org.scalatest"           %% "scalatest"                % "2.2.1" % "test")
