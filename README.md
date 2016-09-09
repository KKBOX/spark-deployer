# spark-deployer

[![Join the chat at https://gitter.im/KKBOX/spark-deployer](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/KKBOX/spark-deployer?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
* A Scala tool which helps deploying [Apache Spark](http://spark.apache.org/) stand-alone cluster on [EC2](http://aws.amazon.com/ec2/) and submitting job.
* Currently supports Spark 2.0.0+.
* There are two modes when using spark-deployer: SBT plugin mode and embedded mode.

## SBT plugin mode

Here are the basic steps to run a Spark job (all the sbt commands support TAB-completion):

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
  addSbtPlugin("net.pishen" % "spark-deployer-sbt" % "3.0.1")
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
  
  (Most settings have default values, just hit Enter to go through it.)

7. Create a cluster with 1 master and 2 workers by:

  ```
  > sparkCreateCluster 2
  ```

8. See your cluster's status by:

  ```
  > sparkShowMachines
  ```

9. Submit your job by:

  ```
  > sparkSubmit
  ```

10. When your job is done, destroy your cluster with

  ```
  > sparkDestroyCluster
  ```

### Advanced functions
* To build config with different name or build a config based on old one:

  ```
  > sparkBuildConfig <new-config-name>
  > sparkBuildConfig <new-config-name> from <old-config-name>
  ```

  All the configs are stored as `.deployer.json` files in the `conf/` folder. You can modify it if you know what you're doing.

* To change the current config:

  ```
  > sparkChangeConfig <config-name>
  ```

* To submit a job with arguments or with a main class:

  ```
  > sparkSubmit <args>
  > sparkSubmitMain mypackage.Main <args>
  ```

* To add or remove worker machines dynamically:

  ```
  > sparkAddWorkers <num-of-workers>
  > sparkRemoveWorkers <num-of-workers>
  ```

## Embedded mode
If you don't want to use sbt, or if you would like to trigger the cluster creation from within your Scala application, you can include the library of spark-deployer directly:
```
libraryDependencies += "net.pishen" %% "spark-deployer-core" % "3.0.1"
```
Then, from your Scala code, you can do something like this:
```scala
import sparkdeployer._

// build a ClusterConf
val clusterConf = ClusterConf.build()

// save and load ClusterConf
clusterConf.save("path/to/conf.deployer.json")
val clusterConfReloaded = ClusterConf.load("path/to/conf.deployer.json")

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

## FAQ

### Could I use other ami?
Yes, just specify the ami id when running `sparkBuildConfig`. The image should be HVM EBS-Backed with Java 7+ installed. You can also run some commands before Spark start on each machine by editing the `preStartCommands` in json config. For example:
```
"preStartCommands": [
  "sudo bash -c \"echo -e 'LC_ALL=en_US.UTF-8\\nLANG=en_US.UTF-8' >> /etc/environment\"",
  "sudo apt-get -qq install openjdk-8-jre",
  "cd spark/conf/ && cp log4j.properties.template log4j.properties && echo 'log4j.rootCategory=WARN, console' >> log4j.properties"
]
```

When using custom ami, the `root device` should be your root volume's name (`/dev/sda1` for Ubuntu) that can be enlarged by `disk size` settings in master and workers.

### Could I use custom Spark tarball?
Yes, just change the tgz url when running `sparkBuildConfig`, the tgz will be extracted as a `spark/` folder in each machine's home folder.

### What rules should I set on my security group?
Assuming your security group id is `sg-abcde123`, the basic settings is:

Type | Protocol | Port Range | Source
---- | -------- | ---------- | ------
All traffic | All | All | `sg-abcde123`
SSH | TCP | 22 | `<your-allowed-ip>`
Custom TCP Rule | TCP | 8080-8081 | `<your-allowed-ip>`
Custom TCP Rule | TCP | 4040 | `<your-allowed-ip>`

### How do I upgrade the config to new version of spark-deployer?
Use `sparkBuildConfig default from default` to build a new config based on settings in old one. If this doesn't work or you don't mind rebuilding one from scratch, it's recommended to directly create a new config by `sparkBuildConfig`.

### Could I change the directory where configurations are saved?
You can change it by add the following line to your `build.sbt`:
```
sparkConfigDir := "path/to/my-config-dir"
```

## How to contribute
* Please report issue or ask on gitter if you meet any problem.
* Pull requests are welcome.
