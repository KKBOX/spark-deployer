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

import java.io.File
import awscala.ec2.EC2
import awscala.Region0
import com.amazonaws.services.ec2.model.EbsBlockDevice
import com.amazonaws.services.ec2.model.BlockDeviceMapping
import com.amazonaws.services.ec2.model.RunInstancesRequest
import collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import com.amazonaws.services.ec2.model.CreateTagsRequest
import com.amazonaws.services.ec2.model.Tag
import awscala.ec2.Instance
import sys.process._
import scala.concurrent.duration.Duration

class SparkDeployer(val sparkConf: SparkConf) {
  private def ec2 = EC2.at(Region0(sparkConf.region))

  private val masterName = sparkConf.clusterName + "-master"
  private val workerPrefix = sparkConf.clusterName + "-worker"

  //ec2 machine addresses
  private def getInstanceAddress(i: Instance) =
    if (sparkConf.usePrivateIp) {
      i.privateIpAddress
    } else {
      i.publicDnsName
    }

  private def getMyInstances() = ec2.instances.filter(i => i.keyName == sparkConf.keypair && i.state.getName != "terminated")

  private def getMaster() = getMyInstances().filter(i => i.getName.nonEmpty && i.name == masterName).headOption
  private def getWorkers() = getMyInstances().filter(i => i.getName.nonEmpty && i.name.startsWith(workerPrefix))

  //helper
  private def ssh(address: String, remoteCommand: String, failedMessage: String) = {
    val cmd = Seq("ssh", "-i", sparkConf.pem,
      "-o", "StrictHostKeyChecking=no",
      "-o", s"ConnectTimeout=${sparkConf.sshTimeout}",
      "-tt",
      s"ec2-user@$address", remoteCommand)
    println(cmd.mkString(" "))
    val exitValue = cmd.!
    if (exitValue != 0) sys.error(s"[ssh-error] exit:$exitValue - $failedMessage") else exitValue
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
        .withImageId("ami-e7527ed7")
        .withInstanceType(instanceType)
        .withKeyName(sparkConf.keypair)
        .withMaxCount(1)
        .withMinCount(1))
      .map(req => sparkConf.securityGroupIds.map(set => req.withSecurityGroupIds(set.asJava)).getOrElse(req))
      .map(req => sparkConf.subnetId.map(id => req.withSubnetId(id)).getOrElse(req))
      .map {
        req =>
          println(s"[$name] creating instance.")
          blocking(ec2.runAndAwait(req))
      }
      .map {
        instances =>
          if (instances.isEmpty) {
            sys.error(s"[$name] creation failed.")
          } else {
            val instance = instances.head
            val address = getInstanceAddress(instance)

            //name the instance
            println(s"[$name] naming instance.")
            ec2.createTags(new CreateTagsRequest()
              .withResources(instance.instanceId)
              .withTags(new Tag("Name", name)))

            //download spark
            println(s"[$name] downloading spark (connect timeout = ${sparkConf.sshTimeout}s).")
            ssh(address,
              s"wget -nv ${sparkConf.sparkTgzUrl} && tar -zxf ${sparkConf.sparkTgzName}",
              s"[$name] download spark failed.")

            //setup master ip
            println(s"[$name] setting master ip.")
            val sparkEnvPath = sparkConf.sparkDirName + "/conf/spark-env.sh"
            val masterIp = masterIpOpt.getOrElse(address)
            ssh(address,
              s"echo 'SPARK_MASTER_IP=$masterIp' > $sparkEnvPath && chmod u+x $sparkEnvPath",
              s"[$name] set master failed.")

            address
          }
      }
  }

  def createMaster() = {
    assert(getMaster.isEmpty, s"[$masterName] master already exists.")
    val future = createInstance(masterName, sparkConf.masterInstanceType, sparkConf.masterDiskSize, None).map {
      address =>
        //start the master
        println(s"[$masterName] staring master.")
        ssh(address,
          s"./${sparkConf.sparkDirName}/sbin/start-master.sh",
          s"[$masterName] start master failed.")

        println(s"[$masterName] master started.\nWeb UI: http://$address:8080\nLogin command: ssh -i ${sparkConf.pem} ec2-user@$address")
    }
    Await.result(future, Duration.Inf)
  }

  def addWorkers(num: Int) = {
    val masterOpt = getMaster()
    assert(masterOpt.nonEmpty && masterOpt.get.state.getName == "running", "Master does not exist, can't create workers.")
    val masterIp = getInstanceAddress(masterOpt.get)

    val startIndex = getWorkers().filter(_.state.getName != "terminated")
      .map(_.name.split("-").last.toInt)
      .sorted.reverse
      .headOption.getOrElse(0) + 1

    val futures = (startIndex to startIndex + num - 1).map(workerPrefix + "-" + _).map {
      workerName =>
        createInstance(workerName, sparkConf.workerInstanceType, sparkConf.workerDiskSize, Some(masterIp))
          .map {
            address =>
              //start the worker
              println(s"[$workerName] staring worker.")
              ssh(address,
                s"./${sparkConf.sparkDirName}/bin/spark-class org.apache.spark.deploy.worker.Worker spark://$masterIp:7077 &> /dev/null &",
                s"[$workerName] start worker failed.")

              println(s"[$workerName] worker started.")
              workerName -> "Success"
          }
          .recover {
            case e: Exception => workerName -> e.toString()
          }
    }
    val future = Future.sequence(futures)

    val statuses = Await.result(future, Duration.Inf)
    println((Seq("Finished adding workers.") ++ statuses.map(t => t._1 + " -> " + t._2)).mkString("\n"))
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
        println(s"login command: ssh -i ${sparkConf.pem} ec2-user@${getInstanceAddress(master)}")
        println(s"web ui: http://${getInstanceAddress(master)}:8080")
    }

    getWorkers()
      .sortBy(_.name.split("-").last.toInt)
      .foreach {
        worker =>
          println(s"[${worker.name}] ${getInstanceAddress(worker)}")
      }
  }

  def uploadJar(jar: File) = {
    getMaster() match {
      case None =>
        sys.error("No master found.")
      case Some(master) =>
        val masterAddress = getInstanceAddress(master)

        val sshCmd = Seq("ssh", "-i", sparkConf.pem,
          "-o", "StrictHostKeyChecking=no",
          "-o", s"ConnectTimeout=${sparkConf.sshTimeout}").mkString(" ")

        val uploadJarCmd = Seq("rsync",
          "--progress",
          "-ve", sshCmd,
          jar.getAbsolutePath,
          s"ec2-user@$masterAddress:~/job.jar")
        println(uploadJarCmd.mkString(" "))
        if (uploadJarCmd.! != 0) sys.error("[rsync-error] jar upload failed.")

        println(s"Jar uploaded, you can now login master and submit the job. Login command: ssh -i ${sparkConf.pem} ec2-user@$masterAddress")
    }
  }

  def submitJob(jar: File, args: Seq[String]) = {
    uploadJar(jar)

    println("[warning] you're submitting job directly, please make sure you have a stable network connection.")
    getMaster().foreach {
      master =>
        val masterAddress = getInstanceAddress(master)
        val submitJobCmd = Some(Seq(
          s"./${sparkConf.sparkDirName}/bin/spark-submit",
          "--class", sparkConf.mainClass,
          "--master", s"spark://$masterAddress:7077"))
          .map(seq => sparkConf.appName.map(n => seq :+ "--name" :+ n).getOrElse(seq))
          .map(seq => sparkConf.driverMemory.map(m => seq :+ "--driver-memory" :+ m).getOrElse(seq))
          .map(seq => sparkConf.executorMemory.map(m => seq :+ "--executor-memory" :+ m).getOrElse(seq))
          .map(_ :+ "job.jar")
          .map(_ ++ args)
          .get.mkString(" ")

        ssh(masterAddress, submitJobCmd, "job submission failed.")
    }
  }

}