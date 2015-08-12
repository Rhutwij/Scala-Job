# Spark Seed Project #


## Prerequisites ##
SBT must be installed on your system.

## How to Compile ##
```shell
$ sbt compile
```

## How to Test ##

```shell
$ sbt test
```

## How to Create Eclipse Project ##

```shell
$ sbt eclipse
```

## How to Run ##

```shell
$ sbt run
```

Or alternatively, `right-click` on any of the files under the `com.jobs2careers.apps` package and click `run`.


## How to Build a .jar to Submit to Spark or for Zeppelin and other projects ##

```shell
$ sbt assembly
```

## Using [Spark-Submit](https://spark.apache.org/docs/latest/submitting-applications.html) ##
First [download Spark 1.4.0 with Hadoop 2.3](https://spark.apache.org/downloads.html). 
```shell
$ cd ${SPARK_INSTALLATION_DIR}
$ ./bin/spark-submit --class "com.jobs2careers.apps.CountAnimalsJob" --master "local[*]" ${PATH_TO}/spark-seed/target/scala-2.10/spark-seed-assembly-1.0.0.jar
$ ./bin/spark-submit --class "com.jobs2careers.apps.SaveAndLoadRDDJob" --master "local[*]" ${PATH_TO}/spark-seed/target/scala-2.10/spark-seed-assembly-1.0.0.jar
```

You can change the following parameter to run on a Spark Standalone Cluster or YARN:
```shell
--master="spark://ubuntu-master:7077"
--master yarn-client
```