import sbt._
import sbt.Keys._
import Resolvers._
import Dependencies._
import sbtassembly.Plugin._
import sbtassembly.AssemblyUtils._
import AssemblyKeys._
import Keys._
import com.typesafe.sbt.packager.Keys._
import com.typesafe.sbt.SbtNativePackager._

object BuildSettings {
  val projectName = "spark-seed"
  val buildSettings = Seq(
    organization := "com.jobs2careers",
    version := "1.0.0",
    scalaVersion := "2.10.5",
    crossScalaVersions := Seq("2.10.2", "2.10.3", "2.10.4", "2.10.5", "2.11.0", "2.11.1", "2.11.2"),
    scalacOptions ++= Seq()
  )
}

object ApplicationBuild extends Build {

  lazy val main = Project(
    BuildSettings.projectName,
    file("."),
    settings = BuildSettings.buildSettings ++ assemblySettings ++ addArtifact(artifact in (Compile, assembly), assembly)
      ++
      Seq(resolvers := myResolvers,
        libraryDependencies ++= baseDeps,
        mergeStrategy in assembly := mergeFirst
      )
      ++
      Packaging.settings
      ++
      Packaging.server
  ) settings(
    //mainClass in assembly := Some("com.jobs2careers.spark.app.ExecutorApp"),
    excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
      cp filter {_.data.getName == "compile-0.1.0.jar"}
    },
    artifact in (Compile, assembly) ~= { art =>
      art.copy(`classifier` = Some("assembly"))
    },
    fork in run :=true,
    mappings in Universal <++= sourceDirectory  map { src =>
      val resources = src / "main" / "resources"
      val log4j = resources / "log4j.properties"
      val reference = resources / "application.conf"
      Seq(log4j -> "conf/log4j.properties", reference -> "conf/application.conf")
    },
    isSnapshot := true
    )

  lazy val mergeFirst: String => MergeStrategy = {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "jasper", xs @ _*) => MergeStrategy.first
    case PathList("org", "fusesource", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "commons", "beanutils", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "commons", "collections", xs @ _*) => MergeStrategy.first
    case PathList("com", "esotericsoftware", "minlog", xs @ _*) => MergeStrategy.first
    case PathList("org", "eclipse", xs @ _*) => MergeStrategy.first
    case PathList("META-INF", xs @ _*) =>
      (xs map {_.toLowerCase}) match {
        case ("changes.txt" :: Nil ) =>
          MergeStrategy.discard
        case ("manifest.mf" :: Nil) | ("eclipsef.rsa" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
          MergeStrategy.discard
        case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") || ps.last.endsWith("pom.properties") || ps.last.endsWith("pom.xml") =>
          MergeStrategy.discard
        case "plexus" :: xs =>
          MergeStrategy.discard
        case "services" :: xs =>
          MergeStrategy.filterDistinctLines
        case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
          MergeStrategy.filterDistinctLines
        case ps @ (x :: xs) if ps.last.endsWith(".jnilib") || ps.last.endsWith(".dll") =>
          MergeStrategy.first
        case ps @ (x :: xs) if ps.last.endsWith(".txt") =>
          MergeStrategy.discard
        case ("notice" :: Nil) | ("license" :: Nil) | ("mailcap" :: Nil )=>
          MergeStrategy.discard
        case _ => MergeStrategy.deduplicate
      }
    case "application.conf" => MergeStrategy.concat
    case "about.html" => MergeStrategy.discard
    case "plugin.properties" => MergeStrategy.first
    case _ => MergeStrategy.first
  }

}




