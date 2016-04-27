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

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.slf4s.Logging

class ClusterConf(config: Config) {
  val platform = config.as[Option[String]]("platform").getOrElse("ec2")

  val clusterName = config.as[String]("cluster-name")

  val keypair = config.as[String]("keypair")
  val pem = config.as[Option[String]]("pem").map { path =>
    val pemFile = new File(path)
    require(pemFile.exists(), "I can't find your pem file at " + pemFile.getAbsolutePath)
    pemFile.getAbsolutePath
  }

  val user = config.as[Option[String]]("user").getOrElse("ec2-user")

  val addHostIp = config.as[Option[Boolean]]("add-host-ip").getOrElse(false)

  val driverMemory = config.as[Option[String]]("master.driver-memory")
  val executorMemory = config.as[Option[String]]("worker.executor-memory")

  val retryAttempts = config.as[Option[Int]]("retry-attempts").getOrElse(20)

  val sparkTgzUrl = config.as[String]("spark-tgz-url")
  val sparkTgzName = {
    require(sparkTgzUrl.endsWith(".tgz"), "spark-tgz-url should ends with \".tgz\"")
    sparkTgzUrl.split("/").last
  }
  val sparkDirName = sparkTgzName.dropRight(4)

  val mainClass = config.as[Option[String]]("main-class")
  val appName = config.as[Option[String]]("app-name")

  val securityGroupIds = config.as[Option[Set[String]]]("security-group-ids")
  val sparkEnv = config.as[Option[Seq[String]]]("spark-env").getOrElse(Seq.empty)

  val destroyOnFail = config.as[Option[Boolean]]("destroy-on-fail").getOrElse(false)

  val threadPoolSize = config.as[Option[Int]]("thread-pool-size").getOrElse(100)

  val enableS3A = config.as[Option[Boolean]]("enable-s3a").getOrElse(false)
  
  val startupScript = config.as[Option[String]]("startup-script")
}
