import sbt._
import Keys._
import com.typesafe.sbt.packager.Keys._
import com.typesafe.sbt.SbtNativePackager._

object Packaging {


  def app = packageArchetype.java_application

  def server = packageArchetype.java_server

  def settings: Seq[Setting[_]] =
      packagerSettings ++
      deploymentSettings ++
      Seq(
        publishArtifact in (Compile, packageBin) := false,
        publishArtifact in (Universal, packageBin) := true
      )

}
