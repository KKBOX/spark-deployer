# spark-deployer

[![Join the chat at https://gitter.im/KKBOX/spark-deployer](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/KKBOX/spark-deployer?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
* A Scala tool which helps deploying [Apache Spark](http://spark.apache.org/) stand-alone cluster and submitting job.
* Currently supports [Amazon EC2](http://aws.amazon.com/ec2/) and OpenStack with Spark 1.4.1+.
* There are two modes when using spark-deployer, SBT plugin and embedded mode.

## SBT plugin mode
* Set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` if you are deploying Spark on AWS EC2 (or if you want to access AWS S3 from your sbt or Spark cluster).
* Prepare a project with structure like below:
```
project-root
├── build.sbt
├── project
│   └── plugins.sbt
├── spark-deployer.conf
└── src
    └── main
        └── scala
            └── mypackage
                └── Main.scala
```
* Write one line in `project/plugins.sbt`:
```
addSbtPlugin("net.pishen" % "spark-deployer-sbt" % "2.6.0")
```
* Write your cluster configuration in `spark-deployer.conf` (See the [examples](configurations.md) for more details). If you want to use another configuration file name, please set the environment variable `SPARK_DEPLOYER_CONF` when starting sbt (e.g. `$ SPARK_DEPLOYER_CONF=./my-spark-deployer.conf sbt`).
* Write your Spark project's `build.sbt` (Here we give a simple example):
```
lazy val root = (project in file("."))
  .settings(
    name := "my-project-name",
    version := "0.1",
    scalaVersion := "2.10.6",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
    )
  )
```
* Write your job's algorithm in `src/main/scala/mypackage/Main.scala`:
```scala
package mypackage

import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    //setup spark
    val sc = new SparkContext(new SparkConf())
    //your algorithm
    val n = 10000000
    val count = sc.parallelize(1 to n).map { i =>
      val x = scala.math.random
      val y = scala.math.random
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
  }
}
```
* Create the cluster by `sbt "sparkCreateCluster <number-of-workers>"`. You can also execute `sbt` first and type `sparkCreateCluster <number-of-workers>` in the sbt console. You may first type `spark` and hit TAB to see all the available commands.
* Once the cluster is created, submit your job by `sparkSubmitJob <arg0> <arg1> ...`
* When your job is done, destroy your cluster by `sparkDestroyCluster`

### All available commands
* `sparkCreateMaster` creates only the master node.
* `sparkAddWorkers <number-of-workers>` supports dynamically add more workers to an existing cluster.
* `sparkCreateCluster <number-of-workers>` shortcut command for the above two commands.
* `sparkRemoveWorkers <number-of-workers>` supports dynamically remove workers to scale down the cluster.
* `sparkDestroyCluster` terminates all the nodes in the cluster.
* `sparkRestartCluster` restart the cluster with new settings from `spark-env` without recreating the machines.
* `sparkShowMachines` shows the machine addresses with commands to login master and execute spark-shell on it.
* `sparkUploadJar` uploads the job's jar file to master node.
* `sparkSubmitJob` uploads and runs the job.
* `sparkSubmitJobWithMain` uploads and runs the job (with main class provided, e.g. `sparkSubmitJobWithMain mypackage.Main <args>`).
* `sparkRemoveS3Dir <dir-name>` remove the s3 directory with the `_$folder$` folder file (e.g. `sparkRemoveS3Dir s3://bucket_name/middle_folder/target_folder`).

## Embedded mode
If you don't want to use sbt, or if you would like to trigger the cluster creation from within your Scala application, you can include the library of spark-deployer directly:
```
libraryDependencies += "net.pishen" %% "spark-deployer-core" % "2.6.0"
```
Then, from your Scala code, you can do something like this:
```scala
import sparkdeployer._

val sparkDeployer = SparkDeployer.fromFile("path/to/spark-deployer.conf")

val numOfWorkers = 2
val jobJar = new File("path/to/job.jar")
val args = Seq("arg0", "arg1")

sparkDeployer.createCluster(numOfWorkers)
sparkDeployer.submitJob(jobJar, args)
sparkDeployer.destroyCluster()
```

* Environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` should also be set if you need it.
* You may prepare the `job.jar` by sbt-assembly from other sbt project with Spark.
* For other available functions, check `SparkDeployer.scala` in our source code.

spark-deployer uses slf4j, remember to add your own backend to see the log. For example, to print the log on screen, add
```
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.14"
```
