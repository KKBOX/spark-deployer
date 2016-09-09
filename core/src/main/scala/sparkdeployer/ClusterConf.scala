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

import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.typesafe.config.{ ConfigFactory, ConfigRenderOptions }
import org.slf4s.Logging
import play.api.libs.json.{ JsObject, Json }
import scala.collection.JavaConverters._
import better.files._
import com.amazonaws.regions.Regions
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.DescribeImagesRequest
import com.amazonaws.services.ec2.model.Filter
import scalaj.http.Http

case class MachineConf(
  instanceType: String,
  freeMemory: String,
  diskSize: Int,
  spotPrice: Option[String]
)

case class ClusterConf(
  clusterName: String,
  region: String,
  //credentials
  keypair: String,
  pem: Option[String],
  //machine settings
  ami: String,
  user: String,
  rootDevice: String,
  master: MachineConf,
  worker: MachineConf,
  subnetId: Option[String],
  iamRole: Option[String],
  usePrivateIp: Boolean,
  securityGroupIds: Seq[String],
  //spark settings
  sparkTgzUrl: String,
  sparkDir: String,
  //pre-start
  preStartCommands: Seq[String],
  //other settings
  retryAttempts: Int
) {
  def save(path: String) = {
    implicit val mcw = Json.writes[MachineConf]
    implicit val ccw = Json.writes[ClusterConf]
    File(path).overwrite(Json.prettyPrint(Json.toJson(this)))
  }
}

object ClusterConf extends Logging {
  def load(path: String): ClusterConf = {
    implicit val mcr = Json.reads[MachineConf]
    implicit val ccr = Json.reads[ClusterConf]
    Json.parse(File(path).lines.mkString).as[ClusterConf]
  }

  //stdin readers
  private val stdin = System.console()
  private def readLine(name: String, default: Option[String] = None): String = {
    val msg = name + default.fold(": ")(d => s" [$d]: ")
    val res = stdin.readLine(msg).trim
    if (res == "") {
      default match {
        case Some(v) => v
        case None =>
          println("This is a required field, please fill again.")
          readLine(name, default)
      }
    } else res
  }
  private def readLineOption(name: String, default: Option[String] = None) = {
    val res = stdin.readLine(s"$name [${default.getOrElse("None")}]: ").trim
    if (res == "") default else Some(res)
  }
  
  //get instance type information from aws
  //ref: http://stackoverflow.com/questions/7334035/
  case class InstanceType(region: String, name: String, memory: Double, price: String)
  private val instanceTypes = try {
    val j = Json.parse {
      val raw = Http("http://a0.awsstatic.com/pricing/1/ec2/linux-od.min.js")
        .asString.body.split("\n").last.drop(9).dropRight(2)
      ConfigFactory.parseString(raw).root.render(ConfigRenderOptions.concise.setJson(true))
    }.as[JsObject]
    (j \ "config" \ "regions").as[Seq[JsObject]]
      .flatMap { j =>
        val region = (j \ "region").as[String]
        (j \ "instanceTypes").as[Seq[JsObject]]
          .flatMap(j => (j \ "sizes").as[Seq[JsObject]])
          .flatMap { j =>
            val instanceType = (j \ "size").as[String]
            val memory = (j \ "memoryGiB").as[String].toDouble
            (j \ "valueColumns").as[Seq[JsObject]]
              .find(j => (j \ "name").as[String] == "linux")
              .map { j =>
                val price = (j \ "prices" \ "USD").as[String]
                InstanceType(region, instanceType, memory, price)
              }
          }
      }
  } catch {
    case e: Exception =>
      log.warn("Cannot get the instance type information, please report this issue.", e)
      Seq.empty
  }
  private def getFreeMemory(region: String, instanceType: String) = {
    instanceTypes.find(t => t.region == region && t.name == instanceType)
      //spark will reserve 1G for system, we reserve 1.5G here
      .map(t => s"${(t.memory - 1.5).toInt}G")
  }
  private def getSpotPrice(region: String, instanceType: String) = {
    instanceTypes.find(t => t.region == region && t.name == instanceType).map(_.price)
  }
  
  //get ubuntu ami from http://cloud-images.ubuntu.com/locator/ec2/
  case class UbuntuAMI(region: String, name: String, release: String, ami: String)
  private val ubuntuAMIs = try {
    val j = Json.parse {
      val raw = Http("http://cloud-images.ubuntu.com/locator/ec2/releasesTable").asString.body
      ConfigFactory.parseString(raw).root.render(ConfigRenderOptions.concise.setJson(true))
    }.as[JsObject]
    (j \ "aaData").as[Seq[Seq[String]]].filter(_(4) == "hvm:ebs-ssd").map { seq =>
      UbuntuAMI(seq(0), seq(1), seq(5), xml.XML.loadString(seq(6)).text)
    }
  } catch {
    case e: Exception =>
      log.warn("Cannot get Ubuntu AMI, please report this issue.", e)
      Seq.empty
  }

  /** suggested arguments will be ignored if base is not empty */
  def build(
    base: Option[ClusterConf] = None,
    suggestedClusterName: Option[String] = None,
    suggestedSparkVersion: Option[String] = None
  ) = {
    val clusterName = readLine("cluster name", base.map(_.clusterName).orElse(suggestedClusterName))
    
    val region = readLine(
      "region", base.map(_.region)
    )

    val ec2 = new AmazonEC2Client().withRegion[AmazonEC2Client](Regions.fromName(region))
    
    println(s"(You may check https://${region}.console.aws.amazon.com/ec2/v2/home#KeyPairs to fill following settings.)")
    val keypair = readLine("keypair", base.map(_.keypair))
    val pem = readLineOption("identity file (pem)", base.flatMap(_.pem)).map { path =>
      if (path.startsWith("~")) {
        sys.props.get("user.home").getOrElse(sys.error("I can't determine your home folder.")) + path.substring(1)
      } else path
    }

    val (ami, isDefaultAMI) = {
      //find default ubuntu ami
      val defaultAMI = ubuntuAMIs
        .filter(u => u.name == "xenial" && u.region == region)
        .sortBy(_.release)
        .lastOption
        .map(_.ami)
      val res = readLine("ami", base.map(_.ami).orElse(defaultAMI))
      (res, defaultAMI.fold(false)(_ == res))
    }
    val user = if (isDefaultAMI) "ubuntu" else {
      readLine("user name (used to ssh into ec2 machines)", base.map(_.user))
    }
    val rootDevice = if (isDefaultAMI) "/dev/sda1" else {
      readLine("root device", base.map(_.rootDevice))
    }

    println("(You may check https://aws.amazon.com/ec2/pricing/ to fill following settings.)")
    val masterInstanceType = readLine(
      "instance type of master", base.map(_.master.instanceType).orElse(Some("m4.large"))
    )
    val masterFreeMemory = readLine("driver memory", getFreeMemory(region, masterInstanceType))
    val masterDiskSize = readLine(
      "disk size of master (GB)", base.map(_.master.diskSize).orElse(Some(15)).map(_.toString)
    ).toInt
    val masterSpotPrice = readLineOption(
      "spot price of master (enter \"None\" to disable)", None
    ).filterNot(_ == "None")

    val workerInstanceType = readLine(
      "instance type of worker", base.map(_.worker.instanceType).orElse(Some("c4.xlarge"))
    )
    val workerFreeMemory = readLine("executor memory", getFreeMemory(region, workerInstanceType))
    val workerDiskSize = readLine(
      "disk size of worker (GB)", base.map(_.worker.diskSize).orElse(Some(60)).map(_.toString)
    ).toInt
    val workerSpotPrice = readLineOption(
      "spot price of worker (enter \"None\" to disable)", getSpotPrice(region, workerInstanceType)
    ).filterNot(_ == "None")

    println(s"(You may check https://${region}.console.aws.amazon.com/vpc/home#subnets to fill following settings.)")
    val subnetId = readLineOption("subnet id", base.flatMap(_.subnetId))
    
    println("(You may check https://console.aws.amazon.com/iam/home#roles to fill following settings.)")
    val iamRole = readLineOption("IAM role", base.flatMap(_.iamRole))
    
    val usePrivateIp = if (subnetId.nonEmpty) true else readLine(
      "use private ip", base.map(_.usePrivateIp).orElse(Some(true)).map(_.toString)
    ).toBoolean
    
    println(s"(You may check https://${region}.console.aws.amazon.com/ec2/v2/home#SecurityGroups to fill following settings.)")
    val securityGroupIds = readLineOption(
      "security group ids (comma separated)", base.map(_.securityGroupIds.mkString(","))
    ).toSeq.flatMap(_.split(",").map(_.trim))
    
    val (sparkTgzUrl, fixS3A) = {
      val defaultSparkVersion = "2.0.0"
      //only version >= defaultSparkVersion will auto generate the url
      val sparkVersion = suggestedSparkVersion match {
        case None => Some(defaultSparkVersion)
        case Some(v) if v >= defaultSparkVersion => Some(v)
        case Some(v) => None
      }
      val suggestedTgzUrl = sparkVersion
        .map(v => s"http://d3kbcqa49mib13.cloudfront.net/spark-$v-bin-hadoop2.7.tgz")

      val res = readLine("Spark's tarball url", base.map(_.sparkTgzUrl).orElse(suggestedTgzUrl))
      (res, suggestedTgzUrl.fold(false)(_ == res))
    }
    val sparkDir = "spark"
    
    val preStartCommands = Seq.empty[String]
      //fix the encoding problem on ubuntu
      .++(if (isDefaultAMI) Some("sudo bash -c \"echo -e 'LC_ALL=en_US.UTF-8\\nLANG=en_US.UTF-8' >> /etc/environment\"") else None)
      //install java 8
      .++(if (isDefaultAMI) Some("sudo apt-get -qq install openjdk-8-jre") else None)
      //workaround Spark's s3a bug
      //ref: http://deploymentzone.com/2015/12/20/s3a-on-spark-on-aws-ec2/
      .++(if (fixS3A) Some(Seq(
        s"cd $sparkDir/jars/",
        "wget -nv https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar",
        "wget -nv https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.1/hadoop-aws-2.7.1.jar"
      ).mkString(" && ")) else None)
    
    ClusterConf(
      clusterName, region, keypair, pem,
      ami, user, rootDevice,
      MachineConf(masterInstanceType, masterFreeMemory, masterDiskSize, masterSpotPrice),
      MachineConf(workerInstanceType, workerFreeMemory, workerDiskSize, workerSpotPrice),
      subnetId, iamRole, usePrivateIp, securityGroupIds,
      sparkTgzUrl, sparkDir, preStartCommands, 40
    )
  }
}
