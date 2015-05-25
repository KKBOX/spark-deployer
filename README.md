# spark-deployer
* A sbt plugin which helps deploying [Apache Spark](http://spark.apache.org/) stand-alone cluster and submitting job.
* Currently only support [Amazon EC2](http://aws.amazon.com/ec2/).
* Spport up to Spark 1.3.1.
* This project is in the experiment state, the spec may change rapidly in the future.

## How to use this plugin
* Set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for AWS.
* In your sbt project, create `project/plugins.sbt`:
```
addSbtPlugin("net.pishen" % "spark-deployer" % "0.2.0")
```
* Create the configuration file `spark-deployer.conf`:
```
cluster-name = "pishen-spark"

keypair = "pishen"
pem = "/home/pishen/.ssh/pishen.pem"

region = "us-west-2"

master {
  instance-type = "m3.medium"
  # EBS disk-size, in GB, volume type fixed to gp2 SSD now.
  disk-size = 8
  # The driver-memory used at spark-submit (optional)
  driver-memory = "2G"
}

worker {
  instance-type = "c4.xlarge"
  disk-size = 40
  # The executor-memory used at spark-submit (optional)
  executor-memory = "6G"
}

# Max number of attempts trying to connect to the machine. (default to 10, one per minute.)
ssh-connection-attempts = 8

# URL for downloading the pre-built Spark tarball.
spark-tgz-url = "http://d3kbcqa49mib13.cloudfront.net/spark-1.3.1-bin-hadoop1.tgz"

main-class = "core.Main"

# Below are optional settings
app-name = "my-app-name"

security-group-ids = ["sg-xxxxxxxx", "sg-yyyyyyyy"]

subnet-id = "subnet-xxxxxxxx"
use-private-ip = true
```
* More information about `spark-tgz-url`:
  * You may find one URL from Spark's website or host one by yourself.
  * You may choose an older version of Spark or different version of Hadoop, but it's not tested, use at your own risk.
  * The URL must ends with `/<spark-folder-name>.tgz` for the auto deployment to work.
* More information about `security-group-ids`:
  * Since akka use random port to connect with master, the security groups should allow all the traffic between cluster machines.
  * Allow port 22 for SSH login.
  * Allow port 8080, 8081, 4040 for web console.
  * Please check [Spark security page](http://spark.apache.org/docs/latest/security.html#configuring-ports-for-network-security) for more information about port settings.
* Create `build.sbt` (Here we give a simple example):
```
lazy val root = (project in file("."))
  .settings(
    name := "my-project-name",
    version := "0.1",
    scalaVersion := "2.10.5",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.3.1" % "provided"
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
* Create Spark cluster by `sbt "sparkCreateCluster <number-of-workers>"`. You can also execute `sbt` first and type `sparkCreateCluster <number-of-workers>` in the sbt console. You may first type `spark` and hit TAB to see all the available commands.
* Once created, submit your job by `sparkSubmitJob <job-args>`

## Other supported commands
* `sparkCreateMaster`
* `sparkAddWorkers <number-of-workers>` supports dynamically add more workers to an existing cluster.
* `sparkRemoveWorkers <number-of-workers>` supports dynamically remove workers to scale down the cluster.
* `sparkDestroyCluster`
* `sparkShowMachines`
* `sparkUploadJar` uploads the job's jar file to the master.
* `sparkRemoveS3Dir <dir-name>` remove the s3 directory with the `_$folder$` folder file. (ex. `sparkRemoveS3Dir s3://bucket_name/middle_folder/target_folder`)
