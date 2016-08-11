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
import org.slf4s.Logging

case class ClusterConf(
  clusterName: String,
  //credentials
  keypair: String,
  pem: Option[String],
  //machine settings
  region: String,
  ami: String,
  user: String,
  rootDevice: String,
  master: MachineConf,
  worker: MachineConf,
  subnetId: String,
  iamRole: Option[String],
  usePrivateIp: Boolean,
  securityGroupIds: Seq[String],
  //spark settings
  sparkTgzUrl: String,
  sparkEnv: Seq[String],
  enableS3A: Boolean,
  //pre-start
  preStartCommands: Seq[String],
  //other settings
  retryAttempts: Int,
  destroyOnFail: Boolean
)

case class MachineConf(
  instanceType: String,
  freeMemory: String,
  diskSize: Int,
  spotPrice: Option[String]
)

// class ClusterConf(conf: Config) {
//   val clusterName = conf.getString("cluster-name")

//   val keypair = config.as[String]("keypair")
//   val pem = config.as[Option[String]]("pem").map { path =>
//     val pemFile = new File(path)
//     require(pemFile.exists(), "I can't find your pem file at " + pemFile.getAbsolutePath)
//     pemFile.getAbsolutePath
//   }

//   val user = config.as[Option[String]]("user").getOrElse("ec2-user")

//   val addHostIp = config.as[Option[Boolean]]("add-host-ip").getOrElse(false)

//   val driverMemory = config.as[Option[String]]("master.driver-memory")
//   val executorMemory = config.as[Option[String]]("worker.executor-memory")

//   val retryAttempts = config.as[Option[Int]]("retry-attempts").getOrElse(20)

//   val sparkTgzUrl = config.as[String]("spark-tgz-url")
//   val sparkTgzName = {
//     require(sparkTgzUrl.endsWith(".tgz"), "spark-tgz-url should ends with \".tgz\"")
//     sparkTgzUrl.split("/").last
//   }
//   val sparkDirName = sparkTgzName.dropRight(4)

//   val mainClass = config.as[Option[String]]("main-class")
//   val appName = config.as[Option[String]]("app-name")

//   val securityGroupIds = config.as[Option[Set[String]]]("security-group-ids")
//   val sparkEnv = config.as[Option[Seq[String]]]("spark-env").getOrElse(Seq.empty)

//   val destroyOnFail = config.as[Option[Boolean]]("destroy-on-fail").getOrElse(false)

//   val enableS3A = config.as[Option[Boolean]]("enable-s3a").getOrElse(false)
  
//   val startupScript = config.as[Option[Seq[String]]]("startup-script")
// }
