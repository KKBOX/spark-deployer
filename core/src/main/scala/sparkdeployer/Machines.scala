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

import com.amazonaws.services.ec2.model._
import scala.collection.JavaConverters._
import org.slf4s.Logging
import com.amazonaws.regions.Regions
import com.amazonaws.services.ec2.AmazonEC2Client

class Machines(implicit conf: ClusterConf) extends Logging {
  val ec2 = new AmazonEC2Client().withRegion[AmazonEC2Client](Regions.fromName(conf.region))
  
  private def getNonTerminatedInstances() = {
    ec2.describeInstances().getReservations.asScala.flatMap(_.getInstances.asScala).toSeq
      .filter(_.getKeyName == conf.keypair)
      .filter(_.getState.getName != "terminated")
  }
  
  def destroyMachines(ids: Seq[String]): Unit = {
    val existingIds = getNonTerminatedInstances.map(_.getInstanceId).intersect(ids).distinct

    if (existingIds.nonEmpty) {
      log.info(s"[EC2] Terminating ${existingIds.size} instances.")
      ec2.terminateInstances(new TerminateInstancesRequest(existingIds.asJava))
      Retry { i =>
        log.info(s"[EC2] Checking status. Attempts: $i.")
        val nonTerminatedTargetInstances = getNonTerminatedInstances.map(_.getInstanceId).intersect(existingIds)
        assert(nonTerminatedTargetInstances.isEmpty, "[EC2] Some instances are not terminated: " + nonTerminatedTargetInstances.mkString(","))
      }
      log.info("[EC2] All instances are terminated.")
    }
  }
  
  private def getBlockDeviceMapping(diskSize: Int) = new BlockDeviceMapping()
    .withDeviceName(conf.rootDevice)
    .withEbs {
      new EbsBlockDevice()
        .withVolumeSize(diskSize)
        .withVolumeType("gp2")
    }

  private def requestSpotInstances(instanceType: String, diskSize: Int, price: String, number: Int) = {
    val createReq = new RequestSpotInstancesRequest()
      .withSpotPrice(price)
      .withInstanceCount(number)
      .withLaunchSpecification {
        Some(new LaunchSpecification())
          .map {
            _.withBlockDeviceMappings(getBlockDeviceMapping(diskSize: Int))
              .withImageId(conf.ami)
              .withInstanceType(instanceType)
              .withKeyName(conf.keypair)
          }
          .map { ls =>
            conf.iamRole.fold(ls)(name => ls.withIamInstanceProfile(new IamInstanceProfileSpecification().withName(name)))
          }
          .map { ls =>
            ls.withAllSecurityGroups(conf.securityGroupIds.map(id => new GroupIdentifier().withGroupId(id)).asJava)
          }
          .map { ls =>
            conf.subnetId.fold(ls)(id => ls.withSubnetId(id))
          }
          .get
      }

    val spotReqIds = ec2.requestSpotInstances(createReq).getSpotInstanceRequests.asScala.map(_.getSpotInstanceRequestId)

    val instanceIds = try {
      Retry { i =>
        log.info(s"[EC2] Getting instance ids from spot requests. Attempts: $i.")
        val descReq = new DescribeSpotInstanceRequestsRequest().withSpotInstanceRequestIds(spotReqIds.asJava)
        ec2.describeSpotInstanceRequests(descReq).getSpotInstanceRequests.asScala.toSeq
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
      }
    } catch {
      case e: Throwable =>
        log.error(s"[EC2] Spot instance creation timeout. Canceling...")
        
        ec2.cancelSpotInstanceRequests(new CancelSpotInstanceRequestsRequest().withSpotInstanceRequestIds(spotReqIds.asJava))
        
        val descReq = new DescribeSpotInstanceRequestsRequest().withSpotInstanceRequestIds(spotReqIds.asJava)
        val reqs = ec2.describeSpotInstanceRequests(descReq).getSpotInstanceRequests.asScala.toSeq
        destroyMachines(reqs.map(_.getInstanceId).filter(id => id != null && id != ""))
        
        throw e
    }

    //get the Instance objects
    getNonTerminatedInstances().filter(instanceIds contains _.getInstanceId)
  }
  
  private def requestOnDemandInstances(instanceType: String, diskSize: Int, number: Int) = {
    val req = Some(new RunInstancesRequest())
      .map {
        _.withBlockDeviceMappings(getBlockDeviceMapping(diskSize))
          .withImageId(conf.ami)
          .withInstanceType(instanceType)
          .withKeyName(conf.keypair)
          .withMaxCount(number)
          .withMinCount(number)
      }
      .map { req =>
        conf.iamRole.fold(req)(name => req.withIamInstanceProfile(new IamInstanceProfileSpecification().withName(name)))
      }
      .map { req =>
        req.withSecurityGroupIds(conf.securityGroupIds.asJava)
      }
      .map { req =>
        conf.subnetId.fold(req)(id => req.withSubnetId(id))
      }
      .get

    ec2.runInstances(req).getReservation.getInstances.asScala.toSeq
  }

  def createMachines(names: Seq[String], isMaster: Boolean): Seq[Machine] = {
    require(getMachines().map(_.name).intersect(names).isEmpty, "Machine already exists.")
    
    val machineConf = if (isMaster) conf.master else conf.worker
    
    //refill the number of machines to workaround AWS's bug (some returned instance ids will not exist).
    @annotation.tailrec
    def fill(existMachines: Seq[Machine], attempts: Int): Seq[Machine] = {
      val number = names.size - existMachines.size
      
      log.info(s"[EC2] Creating $number instances...")
      val instances = machineConf.spotPrice match {
        case Some(price) => requestSpotInstances(machineConf.instanceType, machineConf.diskSize, price, number)
        case None => requestOnDemandInstances(machineConf.instanceType, machineConf.diskSize, number)
      }
      
      val results: Seq[Either[String, Machine]] = instances.zip(names.diff(existMachines.map(_.name))).map {
        case (instance, name) =>
          val id = instance.getInstanceId
          try {
            Retry { i =>
              log.info(s"[EC2] [$id] Naming instance. Attempts: $i.")
              ec2.createTags(new CreateTagsRequest().withResources(id).withTags(new Tag("Name", name)))
            }

            //retry getting address if the instance exists.
            val address = Retry { i =>
              log.info(s"[EC2] [$id] Getting instance's address. Attempts: $i.")
              getNonTerminatedInstances.find(_.getInstanceId == id) match {
                case Some(instance) =>
                  val address = if (conf.usePrivateIp) instance.getPrivateIpAddress else instance.getPublicDnsName
                  if (address == null || address == "") {
                    sys.error(s"Invalid address: $address")
                  }
                  Some(address)
                case None => None
              }
            }.getOrElse(sys.error("Instance not found when getting address."))

            Right(Machine(id, name, address))
          } catch {
            case e: Throwable =>
              log.warn(s"[EC2] [$id] API error when creating instance.", e)
              Left(id)
          }
      }
      
      //since destroyMachines need to check status, destroy them all together.
      val failedIds = results.collect { case Left(id) => id }
      if (failedIds.nonEmpty) {
        destroyMachines(failedIds)
      }

      val newMachines = results.collect { case Right(machine) => machine }

      if (newMachines.size == number) {
        existMachines ++ newMachines
      } else if (attempts > 1) {
        fill(existMachines ++ newMachines, attempts - 1)
      } else {
        log.error("[EC2] Failed on creating enough instances, destroying existing machines...")
        destroyMachines((existMachines ++ newMachines).map(_.id))
        sys.error("Failed on creating enough instances.")
      }
    }
    
    //only try 3 times for now
    fill(Seq.empty, 3)
  }
  
  def getMachines(): Seq[Machine] = {
    getNonTerminatedInstances.map { i =>
      Machine(
        i.getInstanceId,
        i.getTags().asScala.find(_.getKey == "Name").map(_.getValue).getOrElse(""),
        if (conf.usePrivateIp) i.getPrivateIpAddress else i.getPublicDnsName
      )
    }
  }
}

case class Machine(id: String, name: String, address: String)
