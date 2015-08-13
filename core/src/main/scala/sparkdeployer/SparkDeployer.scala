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

import awscala.Region0
import awscala.ec2.{ EC2, Instance }
import com.amazonaws.services.ec2.model.{ BlockDeviceMapping, CreateTagsRequest, EbsBlockDevice, RunInstancesRequest, Tag }
import java.io.File
import scala.collection.JavaConverters.setAsJavaSetConverter
import scala.concurrent.{ Future, blocking }
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.sys.process.stringSeqToProcess
import scala.collection.JavaConverters._

class SparkDeployer(val clusterConf: ClusterConf) {
  private def ec2 = EC2.at(Region0(clusterConf.region))

  private val masterName = clusterConf.clusterName + "-master"
  private val workerPrefix = clusterConf.clusterName + "-worker"

  //ec2 machine addresses
  private def getInstanceAddress(i: Instance) = {
    val address = if (clusterConf.usePrivateIp) {
      i.privateIpAddress
    } else {
      i.publicDnsName
    }
    if (address == null || address == "") None else Some(address)
  }

  private def getMyInstances() = ec2.instances.filter(i => i.keyName == clusterConf.keypair && i.state.getName != "terminated")

  private def getMaster() = getMyInstances().filter(i => i.getName.nonEmpty && i.name == masterName).headOption
  private def getWorkers() = getMyInstances().filter(i => i.getName.nonEmpty && i.name.startsWith(workerPrefix))

  //helper
  private def ssh(
    address: String,
    remoteCommand: String,
    failedMessage: String,
    allocateTTY: Boolean = false,
    retryConnection: Boolean = false
  ) = blocking {
    def sshWithRetry(attempt: Int): Int = {
      val cmd = Some(Seq("ssh", "-i", clusterConf.pem,
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "StrictHostKeyChecking=no"))
        .map(seq => if (allocateTTY) seq :+ "-tt" else seq)
        .map(_ :+ s"ec2-user@$address" :+ remoteCommand)
        .get
      println(cmd.mkString(" "))
      val exitValue = cmd.!
      if (exitValue == 0) {
        exitValue
      } else {
        val errorMessage = s"[ssh-error] attempt: $attempt - exit: $exitValue - $failedMessage"
        if (retryConnection && attempt < clusterConf.retryAttempts) {
          println(errorMessage)
          Thread.sleep(30000)
          sshWithRetry(attempt + 1)
        } else {
          sys.error(errorMessage)
        }
      }
    }
    sshWithRetry(1)
  }

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
      .map {
        req =>
          println(s"[$name] creating instance.")
          ec2.runInstances(req).getReservation.getInstances.asScala.map(Instance(_)).headOption
      }
      .map {
        case None =>
          sys.error(s"[$name] creation failed.")
        case Some(instance) =>
          //name the instance
          println(s"[$name] naming instance.")
          def nameInstanceWithRetry(attempt: Int): Unit = {
            try {
              ec2.createTags(new CreateTagsRequest()
                .withResources(instance.instanceId)
                .withTags(new Tag("Name", name)))
            } catch {
              case e: Exception =>
                if (attempt < clusterConf.retryAttempts) {
                  println(s"[$name] failed naming instance - attempt: $attempt - ${e.getMessage()}")
                  Thread.sleep(30000)
                  nameInstanceWithRetry(attempt + 1)
                } else {
                  throw e
                }
            }
          }
          nameInstanceWithRetry(1)

          //get the address of instance
          println(s"[$name] getting instance's address.")
          def getInstanceAddressWithRetry(attempt: Int): String = {
            //request the new instance object each time, since the old one may contain empty address.
            val newInstanceObj = ec2.instances.find(_.instanceId == instance.instanceId).getOrElse(instance)
            getInstanceAddress(newInstanceObj).getOrElse {
              val errorMessage = s"[$name] failed getting instance's address - attempt: $attempt"
              if (attempt < clusterConf.retryAttempts) {
                println(errorMessage)
                Thread.sleep(30000)
                getInstanceAddressWithRetry(attempt + 1)
              } else {
                sys.error(errorMessage)
              }
            }
          }
          val address = getInstanceAddressWithRetry(1)

          //download spark
          println(s"[$name] downloading spark.")
          ssh(
            address,
            s"wget -nv ${clusterConf.sparkTgzUrl} && tar -zxf ${clusterConf.sparkTgzName}",
            s"[$name] download spark failed.",
            retryConnection = true
          )

          //setup spark-env
          println(s"[$name] setting spark-env.")
          val sparkEnvPath = clusterConf.sparkDirName + "/conf/spark-env.sh"
          val masterIp = masterIpOpt.getOrElse(address)
          ssh(
            address,
            s"echo -e 'SPARK_MASTER_IP=$masterIp\\nSPARK_PUBLIC_DNS=$address' > $sparkEnvPath && chmod u+x $sparkEnvPath",
            s"[$name] set spark-env failed."
          )

          address
      }
  }

  def createMaster() = {
    assert(getMaster.isEmpty, s"[$masterName] master already exists.")
    val future = createInstance(masterName, clusterConf.masterInstanceType, clusterConf.masterDiskSize, None).map {
      address =>
        //start the master
        println(s"[$masterName] staring master.")
        ssh(
          address,
          s"./${clusterConf.sparkDirName}/sbin/start-master.sh",
          s"[$masterName] start master failed."
        )

        println(s"[$masterName] master started.\nWeb UI: http://$address:8080\nLogin command: ssh -i ${clusterConf.pem} ec2-user@$address")
    }
    Await.result(future, Duration.Inf)
  }

  def addWorkers(num: Int) = {
    val masterOpt = getMaster()
    assert(masterOpt.nonEmpty && masterOpt.get.state.getName == "running", "Master does not exist, can't create workers.")
    val masterIp = getInstanceAddress(masterOpt.get).get

    val startIndex = getWorkers().filter(_.state.getName != "terminated")
      .map(_.name.split("-").last.toInt)
      .sorted.reverse
      .headOption.getOrElse(0) + 1

    val futures = (startIndex to startIndex + num - 1).map(workerPrefix + "-" + _).map {
      workerName =>
        createInstance(workerName, clusterConf.workerInstanceType, clusterConf.workerDiskSize, Some(masterIp))
          .map {
            address =>
              //start the worker
              println(s"[$workerName] staring worker.")
              ssh(
                address,
                s"./${clusterConf.sparkDirName}/bin/spark-class org.apache.spark.deploy.worker.Worker spark://$masterIp:7077 &> /dev/null &",
                s"[$workerName] start worker failed."
              )

              println(s"[$workerName] worker started.")
              workerName -> Left("Success")
          }
          .recover {
            case e: Exception => workerName -> Right(e.toString() + " stacktrace: " + e.getStackTraceString)
          }
    }
    val future = Future.sequence(futures)

    val statuses = Await.result(future, Duration.Inf)

    if (statuses.forall(_._2.isLeft)) {
      println(s"Finished adding ${statuses.size} workers.")
    } else {
      sys.error("Failed on adding some workers. The statuses are:\n" + statuses.mkString("\n"))
    }
  }

  def createCluster(num: Int) = {
    createMaster()
    addWorkers(num)
  }

  def removeWorkers(num: Int) = {
    getWorkers().filter(_.state.getName != "terminated")
      .sortBy(_.name.split("-").last.toInt).reverse
      .take(num)
      .foreach {
        worker =>
          println(s"[${worker.name}] terminating.")
          ec2.terminate(worker)
      }
  }

  def destroyCluster() = {
    getWorkers().filter(_.state.getName != "terminated").foreach {
      worker =>
        println(s"[${worker.name}] terminating.")
        ec2.terminate(worker)
    }

    getMaster().foreach {
      master =>
        println(s"[${master.name}] terminating.")
        ec2.terminate(master)
    }
  }

  def showMachines() = {
    getMaster() match {
      case None =>
        println("No master found.")
      case Some(master) =>
        println(s"[${master.name}]")
        val masterAddress = getInstanceAddress(master).getOrElse("null")
        println(s"login command: ssh -i ${clusterConf.pem} ec2-user@$masterAddress")
        println(s"web ui: http://$masterAddress:8080")
    }

    getWorkers()
      .sortBy(_.name.split("-").last.toInt)
      .foreach {
        worker =>
          println(s"[${worker.name}] ${getInstanceAddress(worker).getOrElse("null")}")
      }
  }

  def uploadJar(jar: File) = {
    getMaster() match {
      case None =>
        sys.error("No master found.")
      case Some(master) =>
        val masterAddress = getInstanceAddress(master).get

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
          sys.error("[rsync-error] jar upload failed.")
        } else {
          println(s"Jar uploaded, you can now login master and submit the job. Login command: ssh -i ${clusterConf.pem} ec2-user@$masterAddress")
        }
    }
  }

  def submitJob(jar: File, args: Seq[String]) = {
    uploadJar(jar)

    println("[warning] you're submitting job directly, please make sure you have a stable network connection.")
    getMaster().foreach {
      master =>
        val masterAddress = getInstanceAddress(master).get
        val submitJobCmd = Some(Seq(
          s"AWS_ACCESS_KEY_ID='${sys.env("AWS_ACCESS_KEY_ID")}'",
          s"AWS_SECRET_ACCESS_KEY='${sys.env("AWS_SECRET_ACCESS_KEY")}'",
          s"./${clusterConf.sparkDirName}/bin/spark-submit",
          "--class", clusterConf.mainClass,
          "--master", s"spark://$masterAddress:7077"
        ))
          .map(seq => clusterConf.appName.map(n => seq :+ "--name" :+ n).getOrElse(seq))
          .map(seq => clusterConf.driverMemory.map(m => seq :+ "--driver-memory" :+ m).getOrElse(seq))
          .map(seq => clusterConf.executorMemory.map(m => seq :+ "--executor-memory" :+ m).getOrElse(seq))
          .map(_ :+ "job.jar")
          .map(_ ++ args)
          .get.mkString(" ")

        ssh(masterAddress, submitJobCmd, "job submission failed.", allocateTTY = true)
    }
  }

  def printSparkShellCmd() = {
    getMaster().foreach {
      master =>
        val masterAddress = getInstanceAddress(master).get
        val openShellCmd = Some(Seq(
          s"AWS_ACCESS_KEY_ID='${sys.env("AWS_ACCESS_KEY_ID")}'",
          s"AWS_SECRET_ACCESS_KEY='${sys.env("AWS_SECRET_ACCESS_KEY")}'",
          s"./${clusterConf.sparkDirName}/bin/spark-shell",
          "--master", s"spark://$masterAddress:7077"
        ))
          .map(seq => clusterConf.driverMemory.map(m => seq :+ "--driver-memory" :+ m).getOrElse(seq))
          .map(seq => clusterConf.executorMemory.map(m => seq :+ "--executor-memory" :+ m).getOrElse(seq))
          .get.mkString(" ")

        val cmd = Some(Seq("ssh", "-i", clusterConf.pem,
          "-o", "UserKnownHostsFile=/dev/null",
          "-o", "StrictHostKeyChecking=no",
          "-tt"))
          .map(_ :+ s"ec2-user@$masterAddress" :+ openShellCmd)
          .get.mkString(" ")
        println(cmd)
    }
  }

}
