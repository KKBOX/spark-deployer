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

import com.typesafe.config.ConfigFactory

import net.ceedubs.ficus.Ficus._

class ClusterConf(configFile: File) {
  val config = ConfigFactory.parseFile(configFile).resolve()

  val clusterName = config.as[String]("cluster-name")

  val keypair = config.as[String]("keypair")
  val pem = {
    val pemFile = new File(config.as[String]("pem"))
    require(pemFile.exists(), "I can't find your pem file at " + pemFile.getAbsolutePath)
    pemFile.getAbsolutePath
  }

  val region = config.as[String]("region")

  val masterInstanceType = config.as[String]("master.instance-type")
  val masterDiskSize = config.as[Int]("master.disk-size")
  val driverMemory = config.as[Option[String]]("master.driver-memory")

  val workerInstanceType = config.as[String]("worker.instance-type")
  val workerDiskSize = config.as[Int]("worker.disk-size")
  val executorMemory = config.as[Option[String]]("worker.executor-memory")

  val sshConnectionAttempts = config.as[Option[Int]]("ssh-connection-attempts").getOrElse(10)

  val sparkTgzUrl = config.as[String]("spark-tgz-url")
  val sparkTgzName = {
    require(sparkTgzUrl.endsWith(".tgz"), "spark-tgz-url should ends with \".tgz\"")
    sparkTgzUrl.split("/").last
  }
  val sparkDirName = sparkTgzName.dropRight(4)

  val mainClass = config.as[String]("main-class")
  val appName = config.as[Option[String]]("app-name")

  val subnetId = config.as[Option[String]]("subnet-id")
  val usePrivateIp = config.as[Option[Boolean]]("use-private-ip").getOrElse(false)
  val securityGroupIds = config.as[Option[Set[String]]]("security-group-ids")
}

object ClusterConf {
  def fromFile(configFile: File) = new ClusterConf(configFile)
  def fromFile(configPath: String) = new ClusterConf(new File(configPath))
}