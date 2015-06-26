Spark Seed Project


# Requirements #
SBT must be installed on your system.


## How to Run ##

```shell
$ sbt run
```

## How to Build a .jar to Submit to Spark ##

```shell
$ sbt assembly
```

## Using [Spark-Submit](https://spark.apache.org/docs/latest/submitting-applications.html) ##
First [download Spark 1.4.0 with Hadoop 2.3](https://spark.apache.org/downloads.html). 
```shell
$ cd ${SPARK_INSTALLATION_DIR}
$ ./bin/spark-submit --class com.jobs2careers.apps.CountAnimalsJob --master local[*] ${PATH_TO}/spark-seed/target/scala-2.10/spark-seed-assembly-1.0.0.jar

```

Add parameter to Run on Spark Standalone Cluster

--master="spark://ubuntu-master:7077"

Add parameter to Run on Spark YARN Cluster

--master yarn-client