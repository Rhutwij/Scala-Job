name := "spark-seed"

organization := "com.jobs2careers"

version := "0.0.1"

scalaVersion := "2.10.5"

val sparkVersion = "1.3.1"

// managed libraries
libraryDependencies ++= Seq(
    "org.apache.spark"    %%  "spark-core"            % sparkVersion,
    "org.apache.spark"    %%  "spark-sql"             % sparkVersion,
    "net.debasishg" %% "redisclient" % "3.0",
    "com.typesafe.play" % "play-json_2.10" % "2.2.1"
)

// tests
libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "2.2.4" % "test" // Apache v2
)

// don't load multiple spark contexts at the same time
parallelExecution in Test := false

// other configuration
EclipseKeys.withSource := true

