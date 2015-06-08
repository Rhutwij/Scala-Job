import sbt._

object Resolvers {
  val local =    Resolver.defaultLocal
  var mavenLocal = Resolver.mavenLocal
  val maven =    "MAVEN repo"         at "http://repo1.maven.org/maven2"
  val sonatype = "sonatype releases"  at "https://oss.sonatype.org/content/repositories/releases/"
  val sona_snap ="sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
  val typsafe =  "typesafe repo"      at "http://repo.typesafe.com/typesafe/releases/"
  val spary =    "spray repo"         at "http://repo.spray.io/"
  val spary2 =   "Spray repo second"  at "http://repo.spray.cc/"
  val akka =     "Akka repo"          at "http://repo.akka.io/releases/"

  val myResolvers = Seq (local, mavenLocal, maven, sonatype, sona_snap, typsafe, spary, spary2, akka)
}

