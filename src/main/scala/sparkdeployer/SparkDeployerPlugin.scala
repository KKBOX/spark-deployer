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

import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.typesafe.config.Config

import awscala.s3.Bucket
import awscala.s3.S3
import sbt.AutoPlugin
import sbt.Def.macroValueIT
import sbt.Def.spaceDelimited
import sbt.inputKey
import sbt.parserToInput
import sbt.taskKey
import sbtassembly.AssemblyKeys.assembly

object SparkDeployerPlugin extends AutoPlugin {

  object autoImport {
    lazy val sparkClusterConf = taskKey[Config]("Raw configuration.")

    lazy val sparkCreateMaster = taskKey[Unit]("Create master.")
    lazy val sparkAddWorkers = inputKey[Unit]("Add workers.")
    lazy val sparkCreateCluster = inputKey[Unit]("Create master first, then add workers.")

    lazy val sparkRemoveWorkers = inputKey[Unit]("Remove worker.")
    lazy val sparkDestroyCluster = taskKey[Unit]("Destroy cluster.")

    lazy val sparkShowMachines = taskKey[Unit]("Show the addresses of machines.")

    lazy val sparkUploadJar = taskKey[Unit]("Upload job jar to master.")
    lazy val sparkSubmitJob = inputKey[Unit]("Upload and run the job directly.")

    //lazy val sparkLoginMaster = taskKey[Unit]("Login master with ssh.")
    //lazy val sparkShowSpaceUsage = taskKey[Unit]("Show space usage for all the instances.")

    lazy val sparkRemoveS3Dir = inputKey[Unit]("Remove the s3 directory include _$folder$ postfix file.")
  }
  import autoImport._
  override def trigger = allRequirements

  lazy val sparkDeployer = new SparkDeployer(ClusterConf.fromFile("spark-deployer.conf"))

  override lazy val projectSettings = Seq(
    sparkClusterConf := sparkDeployer.clusterConf.config,

    sparkCreateMaster := sparkDeployer.createMaster(),
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
    sparkDestroyCluster := sparkDeployer.destroyCluster(),

    sparkShowMachines := sparkDeployer.showMachines(),

    sparkUploadJar := sparkDeployer.uploadJar(assembly.value),
    sparkSubmitJob := sparkDeployer.submitJob(assembly.value, spaceDelimited().parsed),

    sparkRemoveS3Dir := {
      val args = spaceDelimited().parsed
      require(args.length == 1, "Please give the directory name.")
      val path = args.head
      require(path.startsWith("s3://"), "Path should start with s3://")
      val bucket = Bucket(path.drop(5).takeWhile(_ != '/'))
      val dirPrefix = {
        val raw = path.drop(5).dropWhile(_ != '/').tail
        if (raw.endsWith("/")) raw else raw + "/"
      }

      val s3 = S3()
      s3.keys(bucket, dirPrefix).grouped(1000).foreach {
        keys =>
          val res = s3.deleteObjects(new DeleteObjectsRequest(bucket.getName).withKeys(keys: _*))
          println(res.getDeletedObjects.size() + " objects deleted.")
      }
      s3.get(bucket, dirPrefix.init + "_$folder$").foreach {
        obj =>
          s3.deleteObject(obj)
          println(obj.getKey + " deleted.")
      }
    })
}