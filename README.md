# spark-deployer

[![Join the chat at https://gitter.im/pishen/spark-deployer](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/pishen/spark-deployer?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
* A Scala tool which helps deploying [Apache Spark](http://spark.apache.org/) stand-alone cluster and submitting job.
* Currently supports [Amazon EC2](http://aws.amazon.com/ec2/) with Spark 1.4.1+.
* This project contains three parts, a core library, a SBT plugin, and a simple command line tool.
* Since we're in the experiment state, the spec may change rapidly in the future.

## How to use the SBT plugin
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
addSbtPlugin("net.pishen" % "spark-deployer-sbt" % "0.8.0")
```
* Write your cluster configuration in `spark-deployer.conf` (see the [example](#cluster-configuration-file) below).
* Write your Spark project's `build.sbt` (Here we give a simple example):
```
lazy val root = (project in file("."))
  .settings(
    name := "my-project-name",
    version := "0.1",
    scalaVersion := "2.10.5",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"
    )
  )
```
* Write your job's algorithm in `src/main/scala/mypackage/Main.scala`:
```scala
package mypackage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Main {
  def main(args: Array[String]) {
    //setup spark
    val sc = new SparkContext(new SparkConf())
    //your algorithm 
    sc.textFile("s3n://my-bucket/input.gz").collect().foreach(println)
  }
}
```
* Create the cluster by `sbt "sparkCreateCluster <number-of-workers>"`. You can also execute `sbt` first and type `sparkCreateCluster <number-of-workers>` in the sbt console. You may first type `spark` and hit TAB to see all the available commands.
* Once the cluster is created, submit your job by `sparkSubmitJob <job-args>`

### Other available commands
* `sparkCreateMaster` creates only the master node.
* `sparkAddWorkers <number-of-workers>` supports dynamically add more workers to an existing cluster.
* `sparkRemoveWorkers <number-of-workers>` supports dynamically remove workers to scale down the cluster.
* `sparkDestroyCluster` terminates all the nodes in the cluster.
* `sparkShowMachines` shows the machine addresses with commands to login master and execute spark-shell on it.
* `sparkUploadJar` uploads the job's jar file to master node.
* `sparkRemoveS3Dir <dir-name>` remove the s3 directory with the `_$folder$` folder file. (ex. `sparkRemoveS3Dir s3://bucket_name/middle_folder/target_folder`)

## How to use the command line tool
For the environment that couldn't install sbt, here is a command line tool (jar file) which only requires java installed.
* Set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for AWS.
* Clone this project, build the jar file by `sbt cmd/assembly`. Get the output file at `cmd/target/scala-2.10/spark-deployer-cmd-assembly-x.x.x.jar`.
* Provide the [cluster configuration file](#cluster-configuration-file) `spark-deployer.conf` at the same directory with `spark-deployer-cmd-assembly-x.x.x.jar`.
* Provide a Spark job's jar file `spark-job.jar` (The jar file built by sbt-assembly from a Spark project).
* Use the following commands to create cluster, submit job, and destroy the cluster:
```
java -jar spark-deployer-cmd-assembly-x.x.x.jar --create-cluster <number-of-workers>
java -jar spark-deployer-cmd-assembly-x.x.x.jar --submit-job spark-job.jar <job-args>
java -jar spark-deployer-cmd-assembly-x.x.x.jar --destroy-cluster
```
* To add more workers, use:
```
java -jar spark-deployer-cmd-assembly-x.x.x.jar --add-workers <number-of-workers>
```

## Cluster configuration file
Here we give an example of `spark-deployer.conf` (settings commented out with `#` are optional):
```
cluster-name = "pishen-spark"

keypair = "pishen"
pem = "/home/pishen/.ssh/pishen.pem"

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

spark-tgz-url = "http://d3kbcqa49mib13.cloudfront.net/spark-1.5.0-bin-hadoop1.tgz"

main-class = "mypackage.Main"

# app-name = "my-app-name"

# security-group-ids = ["sg-xxxxxxxx", "sg-yyyyyyyy"]

# subnet-id = "subnet-xxxxxxxx"
# use-private-ip = true
```
* You can provide your own `ami`, the image should be HVM EBS-Backed with Java 7+ installed.
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
