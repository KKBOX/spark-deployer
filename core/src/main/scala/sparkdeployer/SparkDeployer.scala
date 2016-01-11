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
import java.util.concurrent.Executors
import org.slf4s.Logging
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.sys.process.stringSeqToProcess
import scala.util.{Failure, Success, Try}

class SparkDeployer(val clusterConf: ClusterConf) extends Logging {
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(clusterConf.threadPoolSize))

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
    errorMessage: Option[String] = None,
    includeAWSCredentials: Boolean = false
  ) {
    def withRemoteCommand(cmd: String) = this.copy(remoteCommand = Some(cmd))
    def withTTY = this.copy(ttyAllocated = true)
    def withRetry = this.copy(retryEnabled = true)
    def withRunningMessage(msg: String) = this.copy(runningMessage = Some(msg))
    def withErrorMessage(msg: String) = this.copy(errorMessage = Some(msg))
    def withAWSCredentials = this.copy(includeAWSCredentials = true)

    private def fullCommandSeq(maskAWS: Boolean) = Seq(
      "ssh",
      "-i", clusterConf.pem,
      "-o", "UserKnownHostsFile=/dev/null",
      "-o", "StrictHostKeyChecking=no"
    )
      .++(if (ttyAllocated) Some("-tt") else None)
      .:+(s"ec2-user@$address")
      .++(remoteCommand.map { remoteCommand =>
        if (includeAWSCredentials) {
          Seq(
            "AWS_ACCESS_KEY_ID='" + (if (maskAWS) "*" else sys.env("AWS_ACCESS_KEY_ID")) + "'",
            "AWS_SECRET_ACCESS_KEY='" + (if (maskAWS) "*" else sys.env("AWS_SECRET_ACCESS_KEY")) + "'",
            remoteCommand
          ).mkString(" ")
        } else remoteCommand
      })

    def getCommand = fullCommandSeq(true).mkString(" ")

    def run(): Int = retry({ attempts =>
      log.info(runningMessage.getOrElse("ssh") + s" | attempts = $attempts | " + fullCommandSeq(true).mkString(" "))
      val exitValue = fullCommandSeq(false).!
      if (exitValue != 0) {
        sys.error(s"${errorMessage.getOrElse("ssh error")} | exitValue = $exitValue")
      } else exitValue
    }, if (retryEnabled) clusterConf.retryAttempts else 1)
  }

  private def setupSparkEnv(address: String, masterAddressOpt: Option[String], machineName: String) = {
    val sparkEnvPath = clusterConf.sparkDirName + "/conf/spark-env.sh"
    val masterAddress = masterAddressOpt.getOrElse(address)
    val sparkEnvConf = (clusterConf.sparkEnv ++ Seq(s"SPARK_MASTER_IP=$masterAddress", s"SPARK_PUBLIC_DNS=$address")).mkString("\\n")
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
        log.info(s"[$name] Creating instance.")
        ec2.runInstances(req).getReservation.getInstances.asScala.headOption
      }
      .map {
        case None =>
          sys.error(s"Failed creating instance.")
        case Some(instance) =>
          //name the instance
          retry { attempts =>
            log.info(s"[$name] Naming instance | attempts = $attempts")
            ec2.createTags(new CreateTagsRequest()
              .withResources(instance.getInstanceId)
              .withTags(new Tag("Name", name)))
          }

          //get the address of instance
          //TODO may not get the address here, need more testing
          val address = retry { attempts =>
            log.info(s"[$name] Getting instance's address | attempts = $attempts")
            getInstances().find(_.nameOpt.map(_ == name).getOrElse(false)) match {
              case None => sys.error("Instance not found when getting address")
              case Some(i) => i.address
            }
          }

          //download spark
          val extractCmd = s"tar -zxf ${clusterConf.sparkTgzName}"
          if (clusterConf.sparkTgzUrl.startsWith("s3://")) {
            val s3Cmd = Seq("aws", "s3", "cp", "--only-show-errors", clusterConf.sparkTgzUrl, "./").mkString(" ")
            SSH(address).withRemoteCommand(s3Cmd + " && " + extractCmd).withAWSCredentials
          } else {
            SSH(address).withRemoteCommand("wget -nv " + clusterConf.sparkTgzUrl + " && " + extractCmd)
          }
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

        log.info(s"[$masterName] Master started.\nWeb UI: http://$address:8080\nLogin command: ssh -i ${clusterConf.pem} ec2-user@$address")
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

            log.info(s"[$workerName] Worker started.")
            workerName -> Try("OK")
          }
          .recover {
            case e: Throwable => workerName -> Try(throw e)
          }
      }
    val future = Future.sequence(futures)

    val statuses = Await.result(future, Duration.Inf)

    if (statuses.forall(_._2.isSuccess)) {
      log.info(s"Finished adding ${statuses.size} worker(s).")
    } else {
      log.info("Failed on adding some workers. The statuses are:")
      statuses.foreach {
        case (workerName, Success(v)) =>
          log.info(s"[$workerName] Success")
        case (workerName, Failure(e)) =>
          log.info(s"[$workerName] Failed | $e")
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
      log.info(s"[${worker.nameOpt.get}] Terminating...")
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
      log.info(s"[${master.nameOpt.get}] Terminating...")
      ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(master.getInstanceId))
    }
  }

  def showMachines() = {
    getMasterOpt() match {
      case None =>
        log.info("No master found.")
      case Some(master) =>
        log.info(s"[${master.nameOpt.get}]")
        if (master.state == "running") {
          val masterAddress = master.address
          log.info("Login command: " + SSH(masterAddress).getCommand)
          log.info(s"Web ui: http://$masterAddress:8080")
        } else {
          log.info("Master shutting down.")
        }
    }

    getWorkers().filter(_.state == "running")
      .sortBy(_.nameOpt.get.split("-").last.toInt)
      .foreach { worker =>
        log.info(s"[${worker.nameOpt.get}] ${worker.address}")
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
        log.info(uploadJarCmd.mkString(" "))
        if (uploadJarCmd.! != 0) {
          sys.error("[rsync-error] Failed uploading jar.")
        } else {
          log.info(s"Jar uploaded, you can now login to master and submit the job. Login command: ssh -i ${clusterConf.pem} ec2-user@$masterAddress")
        }
    }
  }

  def submitJob(jar: File, args: Seq[String]) = withFailover {
    uploadJar(jar)

    log.info("You're submitting job directly, please make sure you have a stable network connection.")
    getMasterOpt().foreach { master =>
      val masterAddress = master.address

      val submitJobCmd = Seq(
        s"./${clusterConf.sparkDirName}/bin/spark-submit",
        "--class", clusterConf.mainClass,
        "--master", s"spark://$masterAddress:7077"
      )
        .++(clusterConf.appName.toSeq.flatMap(n => Seq("--name", n)))
        .++(clusterConf.driverMemory.toSeq.flatMap(m => Seq("--driver-memory", m)))
        .++(clusterConf.executorMemory.toSeq.flatMap(m => Seq("--executor-memory", m)))
        .++("job.jar" +: args)
        .mkString(" ")

      SSH(masterAddress)
        .withRemoteCommand(submitJobCmd)
        .withAWSCredentials
        .withTTY
        .withErrorMessage("Job submission failed.")
        .run
    }
  }

}
