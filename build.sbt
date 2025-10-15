ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

val sparkVersion = "3.5.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion

  //logging for future operations deployment
  //"org.slf4j" & "slf4j-api" % "1.7.36")
)

javacOptions ++= Seq("-source", "11", "-target", "11")
//Runtime settings  for future operations deployment
//run / fork := true
//run / javaOptions ++= Seq("-Xmx2G", "-XX:+UseG1GC")

lazy val root = (project in file("."))
  .settings(
    name := "SAssign"
  )

