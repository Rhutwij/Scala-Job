Spark Seed Project


# Requirements #
SBT must be installed on your system.


# How to run #

```shell
$ sbt run
```

How to build binary

```shell
$ sbt assembly
```

Submit to Spark to Run

```shell
$ cd ${SPARK_INSTALLATION_DIR}
$ ./bin/spark-submit --class com.jobs2careers.apps.CountAnimalsJob --master local[*] ${PATH_TO}/spark-seed/target/scala-2.10/spark-seed-assembly-1.0.0.jar

```

>bin/spark-submit --class com.jobs2careers.ExecutorApp {YOUR DIRECTORY}/spark-seed-assembly-1.0.0.jar com.jobs2careers.task.CountLinesOfKeywordApp

Add parameter to Run on Spark Standalone Cluster

--master="spark://ubuntu-master:7077"

Add parameter to Run on Spark YARN Cluster

--master yarn-client