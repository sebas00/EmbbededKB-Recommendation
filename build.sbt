import AssemblyKeys._

assemblySettings

name := "embedded-knowledge-recommendation"

organization := "io.prediction"

parallelExecution in Test := false
scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
 "org.apache.predictionio" %% "apache-predictionio-core" % "0.10.0-incubating",
  "org.apache.spark"        %% "spark-core"               % "1.4.1" % "provided",
"org.apache.spark" %% "spark-mllib" % "1.4.1" % "provided",
"org.scalatest"           %% "scalatest"                % "2.2.1" % "test")
