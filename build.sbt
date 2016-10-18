import AssemblyKeys._

assemblySettings

name := "embedded-knowledge-recommendation"

organization := "io.prediction"

parallelExecution in Test := false
scalaVersion := "2.10.6"
libraryDependencies ++= Seq(
  "org.apache.predictionio"    %% "apache-predictionio-core"          % pioVersion.value % "provided",
  "org.apache.spark" %% "spark-core"    % "1.3.1" % "provided",
"org.apache.spark" %% "spark-mllib" % "1.3.1" % "provided"
)