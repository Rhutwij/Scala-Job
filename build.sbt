name := "all-spark"

organization := "com.jobs2careers"

version := "0.0.1"

scalaVersion := "2.10.5"

val sparkVersion = "1.4.1"

//Resolvers
resolvers ++= Seq(
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases/",
  "rediscala" at "http://dl.bintray.com/etaty/maven"
)

// managed libraries
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "com.etaty.rediscala" %% "rediscala" % "1.4.0",
    "com.typesafe.play" % "play-json_2.10" % "2.2.1",
    "com.typesafe" % "config" % "1.3.0",
    "joda-time" % "joda-time" % "2.8.1"
)

// tests
libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "2.2.4" % "test" // Apache v2
)

// don't load multiple spark contexts at the same time
parallelExecution in Test := false

// other configuration
EclipseKeys.withSource := true

// run only unitest
testOptions in Test := Seq(Tests.Filter(s => s.endsWith("Spec")))

// https://github.com/sbt/sbt-assembly#excluding-scala-library-jars
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
