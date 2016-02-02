# spark-deployer

[![Join the chat at https://gitter.im/KKBOX/spark-deployer](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/KKBOX/spark-deployer?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
* A Scala tool which helps deploying [Apache Spark](http://spark.apache.org/) stand-alone cluster and submitting job.
* Currently supports [Amazon EC2](http://aws.amazon.com/ec2/) with Spark 1.4.1+.
* There are two modes when using spark-deployer, SBT plugin and embedded mode.

## SBT plugin mode
* Set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for AWS.
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
addSbtPlugin("net.pishen" % "spark-deployer-sbt" % "1.1.0")
```
* Write your cluster configuration in `spark-deployer.conf` (see the [example](#cluster-configuration-file) below).
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
* `sparkRemoveS3Dir <dir-name>` remove the s3 directory with the `_$folder$` folder file. (ex. `sparkRemoveS3Dir s3://bucket_name/middle_folder/target_folder`)

## Embedded mode
If you don't want to use sbt, or if you would like to trigger the cluster creation from within your Scala application, you can include the library of spark-deployer directly:
```
libraryDependencies += "net.pishen" %% "spark-deployer-core" % "1.1.0"
```
Then, from your Scala code, you can do something like this:
```scala
import sparkdeployer._

val sparkDeployer = new SparkDeployer(ClusterConf.fromFile("path/to/spark-deployer.conf"))

val numOfWorkers = 2
val jobJar = new File("path/to/job.jar")
val args = Seq("arg0", "arg1")

sparkDeployer.createCluster(numOfWorkers)
sparkDeployer.submitJob(jobJar, args)
sparkDeployer.destroyCluster()
```

* Environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` should be set in this mode, too.
* You may prepare the `job.jar` by sbt-assembly from other sbt project with Spark.
* For other available functions, check `SparkDeployer.scala` in our source code.

spark-deployer uses slf4j, remember to add your own backend to see the log. For example, to print the log on screen, add
```
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.14"
```

## Cluster configuration file
Here we give an example of `spark-deployer.conf` (settings commented out with `#` are optional):
```
cluster-name = "pishen-spark"

keypair = "pishen"
pem = "/home/pishen/.ssh/pishen.pem"

# user = "ec2-user"

region = "us-west-2"

# ami = "ami-9ff7e8af"

master {
  instance-type = "c4.large"
  disk-size = 8
  driver-memory = "2G"
}

worker {
  instance-type = "c4.xlarge"
  disk-size = 40
  executor-memory = "6G"
}

# retry-attempts = 20

spark-tgz-url = "http://d3kbcqa49mib13.cloudfront.net/spark-1.6.0-bin-hadoop2.4.tgz"

main-class = "mypackage.Main"

# app-name = "my-app-name"

# security-group-ids = ["sg-xxxxxxxx", "sg-yyyyyyyy"]

# subnet-id = "subnet-xxxxxxxx"
# use-private-ip = true

# spark-env = [
#   "SPARK_WORKER_CORES=3",
#   "SPARK_WORKER_MEMORY=6G"
# ]

# destroy-on-fail = true

# thread-pool-size = 100

# enable-s3a = true
```
* You can provide your own `ami`, the image should be HVM EBS-Backed with Java 7+ installed.
* You can provide your own `user`, which will be the username used to login AWS machine.
* Currently tested `instance-type`s are `t2.medium`, `m3.medium`, and `c4.xlarge`. All the M3, M4, C3, and C4 types should work, please report an issue if you encountered a problem.
* Value of `disk-size` is in GB, which should be at least 8. It resets the size of root partition, which is used by both OS and Spark.
* `driver-memory` and `executor-memory` are the memory available for Spark, you may subtract 2G from the physically available memory on that machine.
* Some steps of the deployment (ex. SSH login) may fail at the first time. In default, spark-deployer will retry 10 times before it throw an exception. You can change the number of retries at `retry-attempts`.
* `spark-tgz-url` specifies the location of Spark tarball for each machine to download.
  * You may find one tarball at [Spark Downloads](http://spark.apache.org/downloads.html).
  * To install a different version of Spark, just replace the tarball with the corresponding version.
  * The URL can also be a S3 path starting with `s3://` (If you use a custom ami, please make sure you have `awscli` installed on it).
  * The URL must ends with `/<spark-folder-name>.tgz` for the auto deployment to work.
* `security-group-ids` specify a list of security groups to apply on the machines.
  * Since akka use random port to connect with master, the security groups should allow all the traffic between machines in the cluster.
  * Allow port 22 for SSH login.
  * Allow port 8080, 8081, 4040 for web console (optional).
  * Please check [Spark security page](http://spark.apache.org/docs/latest/security.html#configuring-ports-for-network-security) for more information about port settings.
* `spark-env` adds the additional Spark settings to `conf/spark-env.sh` on each node. Note that `SPARK_MASTER_IP`, `SPARK_MASTER_PORT`, and `SPARK_PUBLIC_DNS` are hard-coded for now.
* `destroy-on-fail`: if set to `true`, destroy the cluster when spark-deployer met an error in `sparkCreateCluster` or `sparkSubmitJob`. Note that you still need to destroy the cluster by yourself if no error happens.
* `enable-s3a`: if set to `true`, add the support for s3a (require hadoop 2.0+). We use the workaround as described [here](http://deploymentzone.com/2015/12/20/s3a-on-spark-on-aws-ec2/).
