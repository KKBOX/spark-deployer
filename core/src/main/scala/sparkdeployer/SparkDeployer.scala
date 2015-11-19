/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sparkdeployer

import com.amazonaws.regions.Regions
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.{BlockDeviceMapping, CreateTagsRequest, EbsBlockDevice, Instance, RunInstancesRequest, Tag, TerminateInstancesRequest}
import java.io.File
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.sys.process.stringSeqToProcess
import scala.util.{Failure, Success, Try}

class SparkDeployer(val clusterConf: ClusterConf) {
  private val ec2 = new AmazonEC2Client().withRegion[AmazonEC2Client](Regions.fromName(clusterConf.region))

  private val masterName = clusterConf.clusterName + "-master"
  private val workerPrefix = clusterConf.clusterName + "-worker"

  implicit class InstanceWrapper(i: Instance) {
    def address = {
      val address = if (clusterConf.usePrivateIp) i.getPrivateIpAddress() else i.getPublicDnsName()
      if (address == null || address == "") sys.error("Instance's address not found") else address
    }
    def state = i.getState().getName()
    def nameOpt = i.getTags().asScala.find(_.getKey == "Name").map(_.getValue)
  }

  //helper functions
  private def getInstances() = {
    ec2.describeInstances()
      .getReservations().asScala.flatMap(_.getInstances.asScala).toSeq
      .filter(i => i.getKeyName == clusterConf.keypair && i.state != "terminated")
  }

  private def getMasterOpt() = {
    getInstances().find(_.nameOpt.map(_ == masterName).getOrElse(false))
  }

  private def getWorkers() = {
    getInstances().filter(_.nameOpt.map(_.startsWith(workerPrefix)).getOrElse(false))
  }

  @annotation.tailrec
  private def retry[T](op: Int => T, attempts: Int): T = {
    Try { op(attempts) } match {
      case Success(x) => x
      case _ if attempts > 1 =>
        Thread.sleep(15000)
        retry(op, attempts - 1)
      case Failure(e) => throw e
    }
  }
  private def retry[T](op: Int => T): T = retry(op, clusterConf.retryAttempts)

  case class SSH(
    address: String,
    remoteCommand: Option[String] = None,
    ttyAllocated: Boolean = false,
    retryEnabled: Boolean = false,
    runningMessage: Option[String] = None,
    errorMessage: Option[String] = None
  ) {
    def withRemoteCommand(cmd: String) = this.copy(remoteCommand = Some(cmd))
    def withTTY = this.copy(ttyAllocated = true)
    def withRetry = this.copy(retryEnabled = true)
    def withRunningMessage(msg: String) = this.copy(runningMessage = Some(msg))
    def withErrorMessage(msg: String) = this.copy(errorMessage = Some(msg))

    def getCommandSeq() = Some(Seq("ssh", "-i", clusterConf.pem,
      "-o", "UserKnownHostsFile=/dev/null",
      "-o", "StrictHostKeyChecking=no"))
      .map(seq => if (ttyAllocated) seq :+ "-tt" else seq)
      .map(_ :+ s"ec2-user@$address")
      .map(_ ++ remoteCommand)
      .get

    def getCommand() = {
      val seq = getCommandSeq()
      if (remoteCommand.isEmpty) seq.mkString(" ") else (seq.init :+ s""""${remoteCommand.get}"""").mkString(" ")
    }

    def run(): Int = retry({ attempts =>
      println(runningMessage.getOrElse("ssh-command") + s" | attempts = $attempts\n${getCommand()}")
      val exitValue = getCommandSeq().!
      if (exitValue != 0) {
        sys.error(s"${errorMessage.getOrElse("ssh error")} | exitValue = $exitValue")
      } else exitValue
    }, if (retryEnabled) clusterConf.retryAttempts else 1)
  }

  private val awsEnvAssignment = Seq(
    s"AWS_ACCESS_KEY_ID='${sys.env("AWS_ACCESS_KEY_ID")}'",
    s"AWS_SECRET_ACCESS_KEY='${sys.env("AWS_SECRET_ACCESS_KEY")}'"
  )

  private def setupSparkEnv(address: String, masterAddressOpt: Option[String], machineName: String) = {
    val sparkEnvPath = clusterConf.sparkDirName + "/conf/spark-env.sh"
    val masterAddress = masterAddressOpt.getOrElse(address)
    val sparkEnvConf = (clusterConf.sparkEnv ++ Map("SPARK_MASTER_IP" -> masterAddress, "SPARK_PUBLIC_DNS" -> address))
      .map { case (k, v) => s"${k}=${v}" }.mkString("\\n")
    SSH(address)
      .withRemoteCommand(s"echo -e '$sparkEnvConf' > $sparkEnvPath && chmod u+x $sparkEnvPath")
      .withRetry
      .withRunningMessage(s"[${machineName}] Setting spark-env")
      .withErrorMessage(s"[${machineName}] Failed setting spark-env")
      .run
  }

  private def runSparkSbin(address: String, scriptName: String, args: Seq[String] = Seq.empty, machineName: String) = {
    SSH(address)
      .withRemoteCommand(s"./${clusterConf.sparkDirName}/sbin/${scriptName} ${args.mkString(" ")}")
      .withRetry
      .withRunningMessage(s"[${machineName}] ${scriptName}")
      .withErrorMessage(s"[${machineName}] Failed ${scriptName}")
      .run
  }

  private def createInstance(name: String, instanceType: String, diskSize: Int, masterAddressOpt: Option[String]) = {
    Future(
      new RunInstancesRequest()
        .withBlockDeviceMappings(new BlockDeviceMapping()
          .withDeviceName("/dev/xvda")
          .withEbs(new EbsBlockDevice()
            .withVolumeSize(diskSize)
            .withVolumeType("gp2")))
        .withImageId(clusterConf.ami)
        .withInstanceType(instanceType)
        .withKeyName(clusterConf.keypair)
        .withMaxCount(1)
        .withMinCount(1)
    )
      .map(req => clusterConf.securityGroupIds.map(set => req.withSecurityGroupIds(set.asJava)).getOrElse(req))
      .map(req => clusterConf.subnetId.map(id => req.withSubnetId(id)).getOrElse(req))
      .map { req =>
        println(s"[$name] Creating instance.")
        ec2.runInstances(req).getReservation.getInstances.asScala.headOption
      }
      .map {
        case None =>
          sys.error(s"Failed creating instance.")
        case Some(instance) =>
          //name the instance
          retry { attempts =>
            println(s"[$name] Naming instance | attempts = $attempts")
            ec2.createTags(new CreateTagsRequest()
              .withResources(instance.getInstanceId)
              .withTags(new Tag("Name", name)))
          }

          //get the address of instance
          //TODO may not get the address here, need more testing
          val address = retry { attempts =>
            println(s"[$name] Getting instance's address | attempts = $attempts")
            getInstances().find(_.nameOpt.map(_ == name).getOrElse(false)) match {
              case None => sys.error("Instance not found when getting address")
              case Some(i) => i.address
            }
          }

          //download spark
          val downloadCmd = if (clusterConf.sparkTgzUrl.startsWith("s3://")) {
            (awsEnvAssignment ++ Seq("aws", "s3", "cp", "--only-show-errors", clusterConf.sparkTgzUrl, "./")).mkString(" ")
          } else {
            "wget -nv " + clusterConf.sparkTgzUrl
          }
          SSH(address)
            .withRemoteCommand(s"$downloadCmd && tar -zxf ${clusterConf.sparkTgzName}")
            .withRetry
            .withRunningMessage(s"[$name] Downloading Spark")
            .withErrorMessage(s"[$name] Failed downloading Spark")
            .run

          setupSparkEnv(address, masterAddressOpt, name)

          address
      }
  }

  private def withFailover[T](op: => T): T = {
    Try { op } match {
      case Success(x) => x
      case Failure(e) =>
        if (clusterConf.destroyOnFail) {
          destroyCluster()
        }
        throw e
    }
  }

  //main functions
  def createMaster() = withFailover {
    assert(getMasterOpt.isEmpty, s"[$masterName] Master already exists.")
    val future = createInstance(masterName, clusterConf.masterInstanceType, clusterConf.masterDiskSize, None).map {
      address =>
        //start the master
        runSparkSbin(address, "start-master.sh", Seq.empty, masterName)

        println(s"[$masterName] Master started.\nWeb UI: http://$address:8080\nLogin command: ssh -i ${clusterConf.pem} ec2-user@$address")
    }
    Await.result(future, Duration.Inf)
  }

  def addWorkers(num: Int) = withFailover {
    val masterOpt = getMasterOpt()
    assert(masterOpt.map(_.state == "running").getOrElse(false), "Master does not exist, can't create workers.")
    val masterAddress = masterOpt.get.address

    val startIndex = getWorkers()
      .flatMap(_.nameOpt)
      .map(_.split("-").last.toInt)
      .sorted.reverse
      .headOption.getOrElse(0) + 1

    val futures = (startIndex to startIndex + num - 1)
      .map(workerPrefix + "-" + _)
      .map { workerName =>
        createInstance(workerName, clusterConf.workerInstanceType, clusterConf.workerDiskSize, Some(masterAddress))
          .map { address =>
            //start the worker
            runSparkSbin(address, "start-slave.sh", Seq(s"spark://$masterAddress:7077"), workerName)

            println(s"[$workerName] Worker started.")
            workerName -> Try("OK")
          }
          .recover {
            case e: Throwable => workerName -> Try(throw e)
          }
      }
    val future = Future.sequence(futures)

    val statuses = Await.result(future, Duration.Inf)

    if (statuses.forall(_._2.isSuccess)) {
      println(s"Finished adding ${statuses.size} worker(s).")
    } else {
      println("Failed on adding some workers. The statuses are:")
      statuses.foreach {
        case (workerName, Success(v)) =>
          println(s"[$workerName] Success")
        case (workerName, Failure(e)) =>
          println(s"[$workerName] Failed | $e")
          e.printStackTrace()
      }
      sys.error("Failed on adding some workers.")
    }
  }

  def createCluster(num: Int) = {
    createMaster()
    addWorkers(num)
  }

  def restartCluster() = {
    val masterOpt = getMasterOpt()
    assert(masterOpt.nonEmpty && masterOpt.get.state == "running", "Master does not exist, can't reload cluster.")
    val masterAddress = masterOpt.get.address

    //setup spark-env
    getWorkers().filter(_.state == "running").++(masterOpt).foreach {
      i => setupSparkEnv(i.address, Some(masterAddress), i.nameOpt.get)
    }

    //stop workers
    getWorkers().filter(_.state == "running").foreach {
      worker => runSparkSbin(worker.address, "stop-slave.sh", Seq.empty, worker.nameOpt.get)
    }

    //stop master
    runSparkSbin(masterAddress, "stop-master.sh", Seq.empty, masterName)

    //start master
    runSparkSbin(masterAddress, "start-master.sh", Seq.empty, masterName)

    //start workers
    getWorkers().filter(_.state == "running").foreach {
      worker => runSparkSbin(worker.address, "start-slave.sh", Seq(s"spark://${masterAddress}:7077"), worker.nameOpt.get)
    }
  }

  private def removeWorkers(workers: Seq[Instance]): Unit = {
    workers.foreach { worker =>
      println(s"[${worker.nameOpt.get}] Terminating...")
      ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(worker.getInstanceId))
    }
  }

  def removeWorkers(num: Int): Unit = removeWorkers {
    getWorkers()
      .sortBy(_.nameOpt.get.split("-").last.toInt).reverse
      .take(num)
  }

  def destroyCluster() = {
    removeWorkers(getWorkers())

    getMasterOpt().foreach { master =>
      println(s"[${master.nameOpt.get}] Terminating...")
      ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(master.getInstanceId))
    }
  }

  //build the command for spark-submit/spark-shell
  private def getSparkCmd(
    masterAddress: String,
    isSparkShell: Boolean,
    args: Seq[String]
  ) = {
    awsEnvAssignment
      .:+(s"./${clusterConf.sparkDirName}/bin/${if (isSparkShell) "spark-shell" else "spark-submit"}")
      .++(if (isSparkShell) Seq.empty else Seq("--class", clusterConf.mainClass))
      .++(Seq("--master", s"spark://$masterAddress:7077"))
      .++(if (isSparkShell) Seq.empty else clusterConf.appName.map(n => Seq("--name", n)).getOrElse(Seq.empty))
      .++(clusterConf.driverMemory.map(m => Seq("--driver-memory", m)).getOrElse(Seq.empty))
      .++(clusterConf.executorMemory.map(m => Seq("--executor-memory", m)).getOrElse(Seq.empty))
      .++(if (isSparkShell) Seq.empty else "job.jar" +: args)
      .mkString(" ")
  }

  def showMachines() = {
    getMasterOpt() match {
      case None =>
        println("No master found.")
      case Some(master) =>
        println(s"[${master.nameOpt.get}]")
        if (master.state == "running") {
          val masterAddress = master.address
          println("Login command: " + SSH(masterAddress).getCommand())
          println(s"Spark-shell command: " + getSparkCmd(masterAddress, true, Seq.empty))
          println(s"Web ui: http://$masterAddress:8080")
        } else {
          println("Master shutting down.")
        }
    }

    getWorkers().filter(_.state == "running")
      .sortBy(_.nameOpt.get.split("-").last.toInt)
      .foreach { worker =>
        println(s"[${worker.nameOpt.get}] ${worker.address}")
      }
  }

  def uploadJar(jar: File) = {
    getMasterOpt() match {
      case None =>
        sys.error("No master found.")
      case Some(master) =>
        val masterAddress = master.address

        val sshCmd = Seq("ssh", "-i", clusterConf.pem,
          "-o", "UserKnownHostsFile=/dev/null",
          "-o", "StrictHostKeyChecking=no").mkString(" ")

        val uploadJarCmd = Seq(
          "rsync",
          "--progress",
          "-ve", sshCmd,
          jar.getAbsolutePath,
          s"ec2-user@$masterAddress:~/job.jar"
        )
        println(uploadJarCmd.mkString(" "))
        if (uploadJarCmd.! != 0) {
          sys.error("[rsync-error] Failed uploading jar.")
        } else {
          println(s"Jar uploaded, you can now login to master and submit the job. Login command: ssh -i ${clusterConf.pem} ec2-user@$masterAddress")
        }
    }
  }

  def submitJob(jar: File, args: Seq[String]) = withFailover {
    uploadJar(jar)

    println("[warning] You're submitting job directly, please make sure you have a stable network connection.")
    getMasterOpt().foreach { master =>
      val masterAddress = master.address
      val submitJobCmd = getSparkCmd(masterAddress, false, args)

      SSH(masterAddress)
        .withRemoteCommand(submitJobCmd)
        .withTTY
        .withErrorMessage("Job submission failed.")
        .run
    }
  }

}
