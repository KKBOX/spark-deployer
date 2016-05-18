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

import awscala.Region0
import awscala.s3.{S3, Bucket}
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.typesafe.config.Config
import sbt._
import sbt.Def.{spaceDelimited, macroValueIT}
import sbt.Keys._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin

object SparkDeployerPlugin extends AutoPlugin {

  object autoImport {
    lazy val sparkDeployerConf = taskKey[Config]("Raw configuration.")
    lazy val sparkChangeConfig = inputKey[Unit]("Change the target key in spark-deployer.conf.")

    lazy val sparkCreateMaster = taskKey[Unit]("Create master.")
    lazy val sparkAddWorkers = inputKey[Unit]("Add workers.")
    lazy val sparkCreateCluster = inputKey[Unit]("Create master first, then add workers.")

    lazy val sparkRemoveWorkers = inputKey[Unit]("Remove worker.")
    lazy val sparkDestroyCluster = taskKey[Unit]("Destroy cluster.")

    lazy val sparkShowMachines = taskKey[Unit]("Show the addresses of machines.")

    lazy val sparkUploadFile = inputKey[Unit]("Upload a file to master.")
    lazy val sparkUploadJar = taskKey[Unit]("Upload job jar to master.")
    lazy val sparkSubmitJob = inputKey[Unit]("Upload and run the job directly.")
    lazy val sparkSubmitJobWithMain = inputKey[Unit]("Upload and run the job directly, with main class specified.")
    
    lazy val sparkRunCommand = inputKey[Unit]("Run a command on master.")
    lazy val sparkRunCommands = inputKey[Unit]("Run a sequence of commands from spark-deployer.conf on master.")

    lazy val sparkRemoveS3Dir = inputKey[Unit]("Remove the s3 directory include _$folder$ postfix file.")
    lazy val sparkRestartCluster = taskKey[Unit]("Restart spark master/worker with new environment variables.")
  }
  import autoImport._
  override def trigger = allRequirements
  override def requires = AssemblyPlugin
  
  private var sparkDeployerKey: String = null
  def sparkDeployer = SparkDeployer.fromFile(sys.env.get("SPARK_DEPLOYER_CONF").getOrElse("spark-deployer.conf"), sparkDeployerKey)
  
  lazy val localModeSettings = Seq(
    run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)),
    runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run)),
    fork := true,
    javaOptions := Seq("-Dspark.master=local[*]", s"-Dspark.app.name=local-app"),
    outputStrategy := Some(StdoutOutput)
  )
  
  override lazy val projectSettings = Seq(
    sparkDeployerConf := sparkDeployer.config,
    sparkChangeConfig := {
      val args = spaceDelimited().parsed
      sparkDeployerKey = args.headOption.getOrElse(null)
    },

    sparkCreateMaster := {
      sparkDeployer.createMaster()
    },
    sparkAddWorkers := {
      val args = spaceDelimited().parsed
      require(args.length == 1, "Usage: sparkAddWorkers <num-of-workers>")

      val numOfWorkers = args.head.toInt
      require(numOfWorkers > 0, "num-of-workers should > 0")

      sparkDeployer.addWorkers(numOfWorkers)
    },
    sparkCreateCluster := {
      val args = spaceDelimited().parsed
      require(args.length == 1, "Usage: sparkCreateCluster <num-of-workers>")

      val numOfWorkers = args.head.toInt
      require(numOfWorkers > 0, "num-of-workers should > 0")

      sparkDeployer.createCluster(numOfWorkers)
    },

    sparkRemoveWorkers := {
      val args = spaceDelimited().parsed
      require(args.length == 1, "Usage: sparkRemoveWorkers <num-of-workers>")

      val numOfWorkers = args.head.toInt
      require(numOfWorkers > 0, "num-of-workers should > 0")

      sparkDeployer.removeWorkers(numOfWorkers)
    },
    sparkDestroyCluster := {
      sparkDeployer.destroyCluster()
    },

    sparkShowMachines := {
      sparkDeployer.showMachines()
    },

    sparkUploadFile := {
      val args = spaceDelimited().parsed
      require(args.size == 2, "Usage: sparkUploadFile <local-path> <remote-path>")
      sparkDeployer.uploadFile(new File(args.head), args.last)
    },
    sparkUploadJar := {
      sparkDeployer.uploadJar(assembly.value)
    },
    sparkSubmitJob := {
      sparkDeployer.submitJob(assembly.value, spaceDelimited().parsed)
    },
    sparkSubmitJobWithMain := {
      val args = spaceDelimited().parsed
      require(args.size > 0, "Usage: sparkSubmitJobWithMain MainClass <args>")
      val mainClass = args.head
      sparkDeployer.submitJob(assembly.value, args.tail, mainClass)
    },

    sparkRunCommand := {
      val args = spaceDelimited().parsed
      val command = args.mkString(" ")
      sparkDeployer.runCommand(command)
    },
    sparkRunCommands := {
      val args = spaceDelimited().parsed
      val configKey = args.head
      sparkDeployer.runCommands(configKey)
    },

    sparkRemoveS3Dir := {
      val log = streams.value.log
      val args = spaceDelimited().parsed
      require(args.length == 1, "Please give the directory name.")
      val path = args.head
      require(path.startsWith("s3://"), "Path should start with s3://")
      val bucket = Bucket(path.drop(5).takeWhile(_ != '/'))
      val dirPrefix = {
        val raw = path.drop(5).dropWhile(_ != '/').tail
        if (raw.endsWith("/")) raw else raw + "/"
      }

      val s3 = S3().at(Region0(S3().location(bucket)))
      s3.keys(bucket, dirPrefix).grouped(1000).foreach {
        keys =>
          val res = s3.deleteObjects(new DeleteObjectsRequest(bucket.getName).withKeys(keys: _*))
          log.info(res.getDeletedObjects.size() + " objects deleted.")
      }
      s3.get(bucket, dirPrefix.init + "_$folder$").foreach {
        obj =>
          s3.deleteObject(obj)
          log.info(obj.getKey + " deleted.")
      }
    },
    sparkRestartCluster := {
      sparkDeployer.restartCluster()
    }
  )
}
