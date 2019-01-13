name := "gtw-weight"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-graphx" % "2.4.0" % "provided",
//  "org.apache.spark" %% "spark-core" % "2.4.0",
//  "org.apache.spark" %% "spark-sql" % "2.4.0",
//  "org.apache.spark" %% "spark-graphx" % "2.4.0",
  "com.github.scopt" %% "scopt" % "3.5.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.9.4",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.4",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.4"
)

assemblyJarName in assembly := "gtw-weight.jar"

test in assembly := {}

mainClass in assembly := Some("io.gtw.infrastructure.weight.Weight")

assemblyMergeStrategy in assembly := {
  case x if x.contains("scala-logging") => MergeStrategy.last
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
