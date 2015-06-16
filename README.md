Spark Seed Project


Requirement

 sbt installed


How to run

Count word Example

>sbt "run com.jobs2careers.task.CountLinesOfKeywordApp"

How to build binary

>sbt clean update compile assembly

Submit to Spark to Run

>bin/spark-submit --class com.jobs2careers.ExecutorApp {YOUR DIRECTORY}/spark-seed-assembly-1.0.0.jar com.jobs2careers.task.CountLinesOfKeywordApp

Add parameter to Run on Spark Standalone Cluster

--master="spark://ubuntu-master:7077"

Add parameter to Run on Spark YARN Cluster

--master yarn-client 