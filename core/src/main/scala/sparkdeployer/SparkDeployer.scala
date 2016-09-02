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
import java.util.concurrent.ForkJoinPool
import org.slf4s.Logging
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.blocking
import scala.concurrent.duration.Duration
import scala.sys.process.stringSeqToProcess
import scala.util.{Failure, Success, Try}

class SparkDeployer(implicit conf: ClusterConf) extends Logging {
  implicit val ec = SparkDeployer.ec

  val masterName = conf.clusterName + "-master"
  val workerPrefix = conf.clusterName + "-worker"
  val machines = new Machines()

  //helper functions
  def getMaster() = machines.getMachines.find(_.name == masterName)
  def getWorkers() = machines.getMachines.filter(_.name.startsWith(workerPrefix)).sortBy(_.name)
  def generateNewWorkerNames(num: Int) = {
    val maxIndex = getWorkers.lastOption.fold(0)(_.name.split("-").last.toInt)
    val start = maxIndex + 1
    val end = maxIndex + num
    (start to end).map(i => s"$workerPrefix-$i").toSeq
  }

  def downloadSpark(machine: Machine) = {
    SSH(machine)
      .withRemoteCommand(s"mkdir -p ${conf.sparkDir} && wget -nv -O - ${conf.sparkTgzUrl} | tar -zx --strip-components 1 -C ${conf.sparkDir}")
      .withRetry
      .run
  }
  
  def runPreStart(machine: Machine) = {
    conf.preStartCommands.foreach { command =>
      SSH(machine).withRemoteCommand(command).withTTY.run
    }
  }
  
  def startMaster(machine: Machine) = {
    SSH(machine).withRemoteCommand(s"./${conf.sparkDir}/sbin/start-master.sh -h ${machine.address}").run
  }
  
  def startWorker(machine: Machine, master: Machine) = {
    SSH(machine).withRemoteCommand(s"./${conf.sparkDir}/sbin/start-slave.sh -h ${machine.address} spark://${master.address}:7077").run
  }

  //TODO make sure ssh key exist in the agent if no pem provided
  
  //main functions
  def createCluster(numOfWorkers: Int) = {
    if (getMaster.isDefined) {
      sys.error("Master already exists.")
    }
    val master = Future {
      val master = machines.createMachines(Seq(masterName), isMaster = true).head
      downloadSpark(master)
      runPreStart(master)
      startMaster(master)
      log.info(s"[${master.name}] Master started.")
      master
    }
    val workers = if (numOfWorkers == 0) {
      Seq.empty[Future[Machine]]
    } else {
      machines.createMachines(generateNewWorkerNames(numOfWorkers), isMaster = false).map {
        worker => Future {
          downloadSpark(worker)
          runPreStart(worker)
        }.flatMap { _ =>
          master.map { master =>
            startWorker(worker, master)
            log.info(s"[${worker.name}] Worker started.")
            worker
          }
        }
      }
    }
    try {
      Await.result(Future.sequence(master +: workers), Duration.Inf)
    } catch {
      case e: Throwable =>
        log.error("Failed on creating cluster, cleaning up.")
        destroyCluster()
    }
  }

  def addWorkers(num: Int) = {
    val master = getMaster.getOrElse(sys.error("Master does not exist."))
    val workers = machines.createMachines(generateNewWorkerNames(num), isMaster = false)
    val processedWorkers = Future.sequence {
      workers.map { worker => 
        Future {
          downloadSpark(worker)
          runPreStart(worker)
          startWorker(worker, master)
          log.info(s"[${worker.name}] Worker started.")
          worker
        }
      }
    }
    try {
      Await.result(processedWorkers, Duration.Inf)
    } catch {
      case e: Throwable =>
        log.error("Failed on adding workers, cleaning up.")
        machines.destroyMachines(workers.map(_.id))
    }
  }

  def removeWorkers(num: Int) = {
    machines.destroyMachines(getWorkers.takeRight(num).map(_.id))
  }

  def destroyCluster() = {
    machines.destroyMachines((getWorkers ++ getMaster).map(_.id))
  }

  def showMachines(): Unit = {
    getMaster match {
      case None =>
        log.info("No master found.")
      case Some(master) =>
        log.info(Seq(
          s"[${master.name}] ${master.address}",
          s"Login command: ${SSH(master).getCommandSeq(true).mkString(" ")}",
          s"Web UI: http://${master.address}:8080"
        ).mkString("\n"))
    }

    getWorkers.foreach { worker =>
      log.info(s"[${worker.name}] ${worker.address}")
    }
  }

  def uploadFile(file: File, targetPath: String) = {
    getMaster match {
      case None =>
        sys.error("No master found.")
      case Some(master) =>
        val sshCmd = Seq("ssh")
          .++(conf.pem.fold(Seq.empty[String])(pem => Seq("-i", pem)))
          .++(Seq(
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "StrictHostKeyChecking=no"
          ))
          .mkString(" ")

        val uploadFileCmd = Seq(
          "rsync",
          "--progress",
          "-ve", sshCmd,
          file.getAbsolutePath,
          s"${conf.user}@${master.address}:${targetPath}"
        )
        log.info(uploadFileCmd.mkString(" "))
        if (uploadFileCmd.! != 0) {
          sys.error("[rsync-error] Failed uploading file.")
        }
    }
  }

  def submit(jar: File, mainClass: String, args: Seq[String]) = {
    uploadFile(jar, "~/job.jar")

    log.warn("You're submitting job directly, please make sure you have a stable network connection.")
    getMaster().foreach { master =>
      val submitJobCmd = Seq(
        s"./${conf.sparkDir}/bin/spark-submit",
        "--master", s"spark://${master.address}:7077",
        "--class", mainClass,
        "--name", conf.clusterName,
        "--driver-memory", conf.master.freeMemory,
        "--executor-memory", conf.worker.freeMemory,
        "job.jar"
      ).++(args).mkString(" ")

      SSH(master)
        .withRemoteCommand(submitJobCmd)
        .withAWSCredentials
        .withTTY
        .run
    }
  }
}

object SparkDeployer {
  val ec = ExecutionContext.fromExecutorService(new ForkJoinPool(100))
}
