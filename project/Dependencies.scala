import sbt._

object Dependencies {
  val spark_version = "1.3.1"
  val hadoop_version = "2.4.0"

  val excludeGuava = ExclusionRule(organization = "com.google.guava")
  val excludeJetty = ExclusionRule(organization = "org.eclipse.jetty")
  val excludeServlet = ExclusionRule(organization = "javax.servlet")
  val baseDeps = Seq (
    /* spark dependencies */
    "org.apache.spark"    %%  "spark-core"            % spark_version  excludeAll(excludeGuava),
    "org.apache.spark"    %%  "spark-sql"             % spark_version  excludeAll(excludeGuava),
    "org.apache.spark"    %%  "spark-streaming"       % spark_version  excludeAll(excludeGuava),
    "org.apache.spark"    %%  "spark-yarn"            % spark_version  excludeAll(excludeGuava),
    "com.google.protobuf" %  "protobuf-java"              % "2.4.1",
    "org.spark-project.protobuf" % "protobuf-java"        % "2.4.1-shaded",    
    /* hadoop dependencies */
    "org.apache.hadoop"   %  "hadoop-client"              % hadoop_version excludeAll(excludeGuava,excludeJetty,excludeServlet),
    /* mysql dependencies */
    "mysql"               % "mysql-connector-java"        % "5.1.35",
    /* other dependencies */
    "com.google.guava"    %  "guava"                      % "11.0.2"
  )
}

