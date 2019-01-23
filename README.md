# Apache Spark (with Pigeon Plugin)

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.

<http://spark.apache.org/>

## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](http://spark.apache.org/documentation.html)
and [project wiki](https://cwiki.apache.org/confluence/display/SPARK).
This README file only contains basic setup instructions.

## Building Spark

Spark is built using [Apache Maven](http://maven.apache.org/).
To build Spark and its example programs, run:

    build/mvn -DskipTests clean package

(You do not need to do this if you downloaded a pre-built package.)
More detailed documentation is available from the project site, at
["Building Spark"](http://spark.apache.org/docs/latest/building-spark.html).
For developing Spark using an IDE, see [Eclipse](https://cwiki.apache.org/confluence/display/SPARK/Useful+Developer+Tools#UsefulDeveloperTools-Eclipse)
and [IntelliJ](https://cwiki.apache.org/confluence/display/SPARK/Useful+Developer+Tools#UsefulDeveloperTools-IntelliJ).

## Interactive Scala Shell

The easiest way to start using Spark is through the Scala shell:

    ./bin/spark-shell

Try the following command, which should return 1000:

    scala> sc.parallelize(1 to 1000).count()

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

    ./bin/pyspark

And run the following command, which should also return 1000:

    >>> sc.parallelize(range(1000)).count()

## Example Programs

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

    ./bin/run-example SparkPi

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be a mesos:// or spark:// URL,
"yarn" to run on YARN, and "local" to run
locally with one thread, or "local[N]" to run locally with N threads. You
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

    MASTER=spark://host:7077 ./bin/run-example SparkPi

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

    ./dev/run-tests

Please see the guidance on how to
[run tests for a module, or individual tests](https://cwiki.apache.org/confluence/display/SPARK/Useful+Developer+Tools).

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version"](http://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

## Pigeon Plugin

Once Spark is successfully built and deployed, along with Pigeon Scheduler installed on each node, one can examine Pigeon for the job scheduling. The following Spark application mimics tasks executions on real productive environment by exacting the latencies from the cluster's trace files for threads sleeping on worker nodes. You may prepare the trace files based on the description [here]().

Currently Pigeon Spark plugin supports a single Spark driver as the Pigeon Scheduler and several Spark executors as the Pigeon masters. Start daemons on scheduler and masters following the description at [Pigeon Scheduler](https://github.com/ruby-/pigeon.git) repo, then start the Spark driver by:
```sh
java -Dspark.driver.host=<hostname_driver> -Dspark.driver.port=60501 -Dspark.driver.memory=1g -Dspark.scheduler=pigeon -Dpigeon.app.name=spark -Dspark.serializer=org.apache.spark.serializer.KryoSerializer -Dspark.broadcast.port=33644 -cp "$SPARK_HOME/jars/*" org.apache.spark.examples.JavaSleep "pigeon@<hostname_driver>:20503" 5 3 <app_name> small "<path_to_trace_file>"
```

At Spark driver started, launch Spark executors at every node that is serving as Pigeon master firstly, then all of its workers using the following command:
```sh
java -Dspark.scheduler=pigeon -Dspark.master.port=7077 -Dspark.hostname=<hostname_of_this_executor> -Dspark.serializer=org.apache.spark.serializer.KryoSerializer -Dspark.kryoserializer.buffer=128 -Dspark.driver.host=<hostname_driver> -Dspark.driver.port=60501 -Dspark.httpBroadcast.uri=http://<hostname_driver>:33644 -Dspark.rpc.message.maxSize=2047 -Dpigeon.app.name=spark -Dpigeon.master.hostname=<hostname_of_this_executor> -cp "$SPARK_HOME/jars/*" org.apache.spark.scheduler.pigeon.PigeonExecutorBackend --driver-url spark://PigeonSchedulerBackend@<hostname_driver>:60501 --worker-type <0_or_1> --cores 1
```

The Spark application (JavaSleep) will be launched to the cluster soon as driver and executors are started.