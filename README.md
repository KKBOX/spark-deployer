# spark-deployer

[![Join the chat at https://gitter.im/KKBOX/spark-deployer](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/KKBOX/spark-deployer?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
* A Scala tool which helps deploying [Apache Spark](http://spark.apache.org/) stand-alone cluster on [EC2](http://aws.amazon.com/ec2/) and submitting job.
* Currently supports Spark 2.0.0+.
* There are two modes when using spark-deployer, SBT plugin mode and embedded mode.

## SBT plugin mode
1. Set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
2. Prepare a project with structure like below:

  ```
  project-root
  ├── build.sbt
  ├── project
  │   └── plugins.sbt
  └── src
      └── main
          └── scala
              └── mypackage
                  └── Main.scala
  ```

3. Add one line in `project/plugins.sbt`:

  ```
  addSbtPlugin("net.pishen" % "spark-deployer-sbt" % "3.0.0")
  ```

4. Write your Spark project's `build.sbt` (Here we give a simple example):

  ```
  name := "my-project-name"
   
  scalaVersion := "2.11.8"
   
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"
  )
  ```

5. Write your job's algorithm in `src/main/scala/mypackage/Main.scala`:

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

6. Enter `sbt`, and build a config by:

  ```
  > sparkBuildConfig
  ```
  
  (Use `sparkBuildConfig <new-config-name>` to build additional config.)

7. Create a cluster with 1 master and 2 workers by:

  ```
  > sparkCreateCluster 2
  ```

8. Submit your job by:

  ```
  > sparkSubmit
  ```
  
  (Use `sparkSubmit <args>` if you have arguments for your job.)

9. When your job is done, destroy your cluster with

  ```
  > sparkDestroyCluster
  ```

## Embedded mode
If you don't want to use sbt, or if you would like to trigger the cluster creation from within your Scala application, you can include the library of spark-deployer directly:
```
libraryDependencies += "net.pishen" %% "spark-deployer-core" % "3.0.0"
```
Then, from your Scala code, you can do something like this:
```scala
import sparkdeployer._

// build a ClusterConf
val clusterConf = ClusterConf.build()

// save and load ClusterConf
clusterConf.save("path/to/conf.json")
val clusterConfReloaded = ClusterConf.load("path/to/conf.json")

// create cluster and submit job
val sparkDeployer = new SparkDeployer()(clusterConf)

val workers = 2
sparkDeployer.createCluster(workers)

val jar = new File("path/to/job.jar")
val mainClass = "mypackage.Main"
val args = Seq("arg0", "arg1")
sparkDeployer.submit(jar, mainClass, args)

sparkDeployer.destroyCluster()
```

* Environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` should also be set.
* You may prepare the `job.jar` by sbt-assembly from other sbt project with Spark.
* For other available functions, check `SparkDeployer.scala` in our source code.

spark-deployer uses slf4j, remember to add your own backend to see the log. For example, to print the log on screen, add
```
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.14"
```
