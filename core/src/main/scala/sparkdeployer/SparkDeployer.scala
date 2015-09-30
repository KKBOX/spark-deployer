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
      val address = if (clusterConf.usePrivateIp) i.getPrivateIpAddress() else i.getPublicIpAddress()
      if (address == null) sys.error("no address found") else address
    }
    def state = i.getState().getName()
    def nameOpt = i.getTags().asScala.find(_.getKey == "Name").map(_.getValue)
  }

  private def getInstances() = ec2.describeInstances()
    .getReservations().asScala.flatMap(_.getInstances.asScala).toSeq
    .filter(i => i.getKeyName == clusterConf.keypair && i.state != "terminated")

  private def getMasterOpt() = getInstances().find(_.nameOpt.map(_ == masterName).getOrElse(false))

  private def getWorkers() = getInstances().filter(_.nameOpt.map(_.startsWith(workerPrefix)).getOrElse(false))

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

  val awsEnvAssignment = Seq(
    s"AWS_ACCESS_KEY_ID='${sys.env("AWS_ACCESS_KEY_ID")}'",
    s"AWS_SECRET_ACCESS_KEY='${sys.env("AWS_SECRET_ACCESS_KEY")}'"
  )

  //main functions
  private def createInstance(name: String, instanceType: String, diskSize: Int, masterIpOpt: Option[String]) = {
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
          val address = instance.address
          println(s"[$name] Got instance address: $address")

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

          //setup spark-env
          val sparkEnvPath = clusterConf.sparkDirName + "/conf/spark-env.sh"
          val masterIp = masterIpOpt.getOrElse(address)
          val envconf = (clusterConf.env ++ Map("SPARK_MASTER_IP" -> masterIp, "SPARK_PUBLIC_DNS" -> address))
            .map { case (k, v) => s"${k}=${v}" }.mkString("\\n")
          SSH(address)
            .withRemoteCommand(s"echo -e '$envconf' > $sparkEnvPath && chmod u+x $sparkEnvPath")
            .withRetry
            .withRunningMessage(s"[$name] Setting spark-env")
            .withErrorMessage(s"[$name] Failed setting spark-env")
            .run

          address
      }
  }

  def createMaster() = {
    assert(getMasterOpt.isEmpty, s"[$masterName] Master already exists.")
    val future = createInstance(masterName, clusterConf.masterInstanceType, clusterConf.masterDiskSize, None).map {
      address =>
        //start the master
        SSH(address)
          .withRemoteCommand(s"./${clusterConf.sparkDirName}/sbin/start-master.sh")
          .withRetry
          .withRunningMessage(s"[$masterName] Starting master")
          .withErrorMessage(s"[$masterName] Failed starting master")
          .run

        println(s"[$masterName] Master started.\nWeb UI: http://$address:8080\nLogin command: ssh -i ${clusterConf.pem} ec2-user@$address")
    }
    Await.result(future, Duration.Inf)
  }

  def addWorkers(num: Int) = {
    val masterOpt = getMasterOpt()
    assert(masterOpt.map(_.state == "running").getOrElse(false), "Master does not exist, can't create workers.")
    val masterIp = masterOpt.get.address

    val startIndex = getWorkers()
      .flatMap(_.nameOpt)
      .map(_.split("-").last.toInt)
      .sorted.reverse
      .headOption.getOrElse(0) + 1

    val futures = (startIndex to startIndex + num - 1)
      .map(workerPrefix + "-" + _)
      .map { workerName =>
        createInstance(workerName, clusterConf.workerInstanceType, clusterConf.workerDiskSize, Some(masterIp))
          .map { address =>
            //start the worker
            SSH(address)
              .withRemoteCommand(s"./${clusterConf.sparkDirName}/sbin/start-slave.sh spark://$masterIp:7077")
              .withRetry
              .withRunningMessage(s"[$workerName] Starting worker")
              .withErrorMessage(s"[$workerName] Failed starting worker")
              .run

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
    val masterOpt = getMaster()
    assert(masterOpt.nonEmpty && masterOpt.get.state.getName == "running", "Master does not exist, can't reload cluster.")
    val masterIp = getInstanceAddress(masterOpt.get).get

    //setup spark-env
    val sparkEnvPath = clusterConf.sparkDirName + "/conf/spark-env.sh"
    (getWorkers().filter(_.state.getName != "terminated")
      .map(worker => getInstanceAddress(worker).get) :+ masterIp).foreach { ip =>
        val envconf = (clusterConf.env ++ Map("SPARK_MASTER_IP" -> masterIp, "SPARK_PUBLIC_DNS" -> ip))
          .map { case (k, v) => s"${k}=${v}" }.mkString("\\n")
        ssh(
          ip,
          s"echo -e '$envconf' > $sparkEnvPath && chmod u+x $sparkEnvPath",
          s"[$ip] set spark-env failed."
        )
      }

    // for slaver stop
    getWorkers().filter(_.state.getName != "terminated").foreach {
      worker =>
        println(s"[${worker.name}] stoping worker.")
        ssh(
          getInstanceAddress(worker).get,
          s"./${clusterConf.sparkDirName}/sbin/stop-slave.sh spark://$masterIp:7077",
          s"[${worker.name}] restarting worker failed."
        )
    }
    // for master
    println(s"[$masterName] restarting master.")
    ssh(
      masterIp,
      s"./${clusterConf.sparkDirName}/sbin/stop-master.sh && ./${clusterConf.sparkDirName}/sbin/start-master.sh",
      s"[$masterName] restart master failed."
    )
    // for slaver starting
    getWorkers().filter(_.state.getName != "terminated").foreach {
      worker =>
        println(s"[${worker.name}] starting worker.")
        ssh(
          getInstanceAddress(worker).get,
          s"./${clusterConf.sparkDirName}/sbin/start-slave.sh spark://$masterIp:7077",
          s"[${worker.name}] restarting worker failed."
        )
    }

  }

  private def removeWorkers(workers: Seq[Instance]): Unit = {
    workers.foreach { worker =>
      println(s"[${worker.nameOpt.get}] Terminating...")
      ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(worker.getInstanceId))
    }
  }

  def removeWorkers(num: Int): Unit = removeWorkers {
    getWorkers().filter(_.state == "running")
      .sortBy(_.nameOpt.get.split("-").last.toInt).reverse
      .take(num)
  }

  def destroyCluster() = {
    removeWorkers(getWorkers().filter(_.state == "running"))

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

  def submitJob(jar: File, args: Seq[String]) = {
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
