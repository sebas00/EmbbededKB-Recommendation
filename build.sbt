import AssemblyKeys._

assemblySettings

name := "embedded-knowledge-recommendation"

organization := "org.apache.predictionio"

parallelExecution in Test := false
scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.predictionio" %% "apache-predictionio-core" % "0.10.0-incubating" % "provided",
  "org.apache.spark"        %% "spark-core"               % "1.4.1" % "provided",
"org.apache.spark" %% "spark-mllib" % "1.4.1" % "provided")
