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

import Helpers.retry
import com.amazonaws.regions.Regions
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.{ BlockDeviceMapping, CancelSpotInstanceRequestsRequest, CreateTagsRequest, DescribeSpotInstanceRequestsRequest, EbsBlockDevice, GroupIdentifier, IamInstanceProfileSpecification, LaunchSpecification, RequestSpotInstancesRequest, RunInstancesRequest, Tag, TerminateInstancesRequest }
import com.typesafe.config.Config
import org.slf4s.Logging
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import net.ceedubs.ficus.Ficus._

class EC2Machines(config: Config) extends Machines with Logging {
  class EC2Conf(config: Config) extends ClusterConf(config) {
    val region = config.as[String]("region")
    val ami = config.as[Option[String]]("ami").getOrElse {
      region match {
        case "us-east-1" => "ami-08111162"
        case "us-west-2" => "ami-c229c0a2"
        case "us-west-1" => "ami-1b0f7d7b"
        case "eu-west-1" => "ami-31328842"
        case "eu-central-1" => "ami-e2df388d"
        case "ap-southeast-1" => "ami-e90dc68a"
        case "ap-northeast-2" => "ami-6598510b"
        case "ap-northeast-1" => "ami-f80e0596"
        case "ap-southeast-2" => "ami-f2210191"
        case "sa-east-1" => "ami-1e159872"
      }
    }
    val rootDevice = config.as[Option[String]]("root-device").getOrElse("/dev/xvda")

    val masterInstanceType = config.as[String]("master.instance-type")
    val masterDiskSize = config.as[Int]("master.disk-size")
    val masterSpotPrice = config.as[Option[String]]("master.spot-price")
    val workerInstanceType = config.as[String]("worker.instance-type")
    val workerDiskSize = config.as[Int]("worker.disk-size")
    val workerSpotPrice = config.as[Option[String]]("worker.spot-price")

    val iamRole = config.as[Option[String]]("iam-role")
    val subnetId = config.as[Option[String]]("subnet-id")
    val usePrivateIp = config.as[Option[Boolean]]("use-private-ip").getOrElse(false)
  }
  implicit val clusterConf = new EC2Conf(config)

  private val ec2 = new AmazonEC2Client().withRegion[AmazonEC2Client](Regions.fromName(clusterConf.region))

  def createMachines(machineType: MachineType, names: Set[String]) = {
    val blockDeviceMapping = new BlockDeviceMapping()
      .withDeviceName(clusterConf.rootDevice)
      .withEbs {
        new EbsBlockDevice()
          .withVolumeSize(machineType match {
            case Master => clusterConf.masterDiskSize
            case Worker => clusterConf.workerDiskSize
          })
          .withVolumeType("gp2")
      }

    def requestSpotInstances(number: Int, price: String) = {
      val req = new RequestSpotInstancesRequest()
        .withSpotPrice(price)
        .withInstanceCount(number)
        .withLaunchSpecification {
          Some(new LaunchSpecification())
            .map {
              _.withBlockDeviceMappings(blockDeviceMapping)
                .withImageId(clusterConf.ami)
                .withInstanceType(machineType match {
                  case Master => clusterConf.masterInstanceType
                  case Worker => clusterConf.workerInstanceType
                })
                .withKeyName(clusterConf.keypair)
            }
            .map { req =>
              clusterConf.iamRole.map(name => req.withIamInstanceProfile(new IamInstanceProfileSpecification().withName(name))).getOrElse(req)
            }
            .map { req =>
              clusterConf.securityGroupIds.map(ids => req.withAllSecurityGroups(ids.map(id => new GroupIdentifier().withGroupId(id)).asJava)).getOrElse(req)
            }
            .map(req => clusterConf.subnetId.map(id => req.withSubnetId(id)).getOrElse(req))
            .get
        }

      val requests = ec2.requestSpotInstances(req).getSpotInstanceRequests.asScala.map(_.getSpotInstanceRequestId)

      val instanceIds = try {
        retry { i =>
          log.info(s"[EC2] Getting instance ids from spot requests. Attempts: $i.")
          val reqreq = new DescribeSpotInstanceRequestsRequest().withSpotInstanceRequestIds(requests.asJava)
          ec2.describeSpotInstanceRequests(reqreq).getSpotInstanceRequests.asScala.toSeq
            .map { req =>
              req.getState match {
                case "open" => sys.error("Spot request still open.")
                case "active" =>
                  val id = req.getInstanceId
                  assert(id != null && id != "")
                  id
                case _ => sys.error("Unhandled spot request state.")
              }
            }
            .toSet
        }
      } catch {
        case e: Throwable =>
          log.error(s"[EC2] Spot instance creation timeout. Canceling...")
          
          ec2.cancelSpotInstanceRequests(new CancelSpotInstanceRequestsRequest().withSpotInstanceRequestIds(requests.asJava))
          
          val reqreq = new DescribeSpotInstanceRequestsRequest().withSpotInstanceRequestIds(requests.asJava)
          val reqs = ec2.describeSpotInstanceRequests(reqreq).getSpotInstanceRequests.asScala.toSeq
          destroyMachines(reqs.map(_.getInstanceId).filter(id => id != null && id != "").toSet)
          
          throw e
      }

      getNonTerminatedInstances().filter(instanceIds contains _.getInstanceId)
    }

    def requestOnDemandInstances(number: Int) = {
      val req = Some(new RunInstancesRequest())
        .map {
          _.withBlockDeviceMappings(blockDeviceMapping)
            .withImageId(clusterConf.ami)
            .withInstanceType(machineType match {
              case Master => clusterConf.masterInstanceType
              case Worker => clusterConf.workerInstanceType
            })
            .withKeyName(clusterConf.keypair)
            .withMaxCount(number)
            .withMinCount(number)
        }
        .map { req =>
          clusterConf.iamRole.map(name => req.withIamInstanceProfile(new IamInstanceProfileSpecification().withName(name))).getOrElse(req)
        }
        .map(req => clusterConf.securityGroupIds.map(ids => req.withSecurityGroupIds(ids.asJava)).getOrElse(req))
        .map(req => clusterConf.subnetId.map(id => req.withSubnetId(id)).getOrElse(req))
        .get

      ec2.runInstances(req).getReservation.getInstances.asScala.toSeq
    }

    //refill the number of machines with retry to workaround AWS's bug.
    @annotation.tailrec
    def fill(existMachines: Seq[Machine], attempts: Int): Seq[Machine] = {
      val number = names.size - existMachines.size

      val spotPriceOpt = machineType match {
        case Master => clusterConf.masterSpotPrice
        case Worker => clusterConf.workerSpotPrice
      }

      log.info(s"[EC2] Creating $number instances...")
      val instances = spotPriceOpt match {
        case Some(price) => requestSpotInstances(number, price)
        case None => requestOnDemandInstances(number)
      }

      val results: Seq[Either[Machine, String]] = instances.zip(names -- existMachines.map(_.name)).map {
        case (instance, name) =>
          val id = instance.getInstanceId
          Try {
            retry { i =>
              log.info(s"[EC2] [$id] Naming instance. Attempts: $i.")
              ec2.createTags(new CreateTagsRequest().withResources(id).withTags(new Tag("Name", name)))
            }

            //retry getting address if the instance exists.
            val address = retry { i =>
              log.info(s"[EC2] [$id] Getting instance's address. Attempts: $i.")
              getNonTerminatedInstances.find(_.getInstanceId == id) match {
                case Some(instance) =>
                  val address = if (clusterConf.usePrivateIp) instance.getPrivateIpAddress else instance.getPublicDnsName
                  if (address == null || address == "") {
                    sys.error(s"Invalid address: $address")
                  }
                  Some(address)
                case None => None
              }
            }.getOrElse(sys.error("Instance not found when getting address."))

            Machine(id, name, address)
          } match {
            case Success(m) => Left(m)
            case Failure(e) =>
              log.warn(s"[EC2] [$id] API error when creating instance.", e)
              Right(id)
          }
      }

      //since destroyMachines need to check status, destroy them all together.
      val failedIds = results.collect { case Right(id) => id }.toSet
      if (failedIds.nonEmpty) {
        destroyMachines(failedIds)
      }

      val newMachines = results.collect { case Left(machine) => machine }

      if (newMachines.size == number) {
        existMachines ++ newMachines
      } else if (attempts > 1) {
        fill(existMachines ++ newMachines, attempts - 1)
      } else {
        sys.error("[EC2] Failed on creating enough instances.")
      }
    }

    //only try 3 times for now
    fill(Seq.empty, 3)
  }

  private def getNonTerminatedInstances() = {
    ec2.describeInstances().getReservations.asScala.flatMap(_.getInstances.asScala).toSeq
      .filter(_.getKeyName == clusterConf.keypair)
      .filter(_.getState.getName != "terminated")
  }

  def destroyMachines(ids: Set[String]) = {
    val existingIds = getNonTerminatedInstances.map(_.getInstanceId).toSet & ids

    if (existingIds.nonEmpty) {
      log.info(s"[EC2] Terminating ${existingIds.size} instances.")
      ec2.terminateInstances(new TerminateInstancesRequest(existingIds.toSeq.asJava))
      retry { i =>
        log.info(s"[EC2] Checking status. Attempts: $i.")
        val nonTerminatedTargetInstances = getNonTerminatedInstances.map(_.getInstanceId).toSet & existingIds
        assert(nonTerminatedTargetInstances.isEmpty, "[EC2] Some instances are not terminated: " + nonTerminatedTargetInstances.mkString(","))
      }
      log.info("[EC2] All instances are terminated.")
    }
  }

  def getMachines() = {
    getNonTerminatedInstances.map { i =>
      Machine(
        i.getInstanceId,
        i.getTags().asScala.find(_.getKey == "Name").map(_.getValue).getOrElse(""),
        if (clusterConf.usePrivateIp) i.getPrivateIpAddress else i.getPublicDnsName
      )
    }
  }
}
