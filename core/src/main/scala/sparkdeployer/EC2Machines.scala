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

import com.amazonaws.regions.Regions
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.{BlockDeviceMapping, CreateTagsRequest, EbsBlockDevice, RunInstancesRequest, Tag, TerminateInstancesRequest}
import org.slf4s.Logging
import scala.collection.JavaConverters._
import scala.util.Try

class EC2Machines(implicit clusterConf: ClusterConf) extends Machines with Logging {
  private val ec2 = new AmazonEC2Client().withRegion[AmazonEC2Client](Regions.fromName(clusterConf.region))

  def createMachines(machineType: MachineType, names: Set[String]) = {
    //refill the number of machines with retry to workaround AWS's bug.
    @annotation.tailrec
    def fill(existMachines: Seq[Machine], attempts: Int): Seq[Machine] = {
      val number = names.size - existMachines.size

      val req = Some(new RunInstancesRequest())
        .map {
          _.withBlockDeviceMappings(new BlockDeviceMapping()
            .withDeviceName("/dev/xvda")
            .withEbs(new EbsBlockDevice()
              .withVolumeSize(machineType match {
                case Master => clusterConf.masterDiskSize
                case Worker => clusterConf.workerDiskSize
              })
              .withVolumeType("gp2")))
            .withImageId(clusterConf.ami)
            .withInstanceType(machineType match {
              case Master => clusterConf.masterInstanceType
              case Worker => clusterConf.workerInstanceType
            })
            .withKeyName(clusterConf.keypair)
            .withMaxCount(number)
            .withMinCount(number)
        }
        .map(req => clusterConf.securityGroupIds.map(ids => req.withSecurityGroupIds(ids.asJava)).getOrElse(req))
        .map(req => clusterConf.subnetId.map(id => req.withSubnetId(id)).getOrElse(req))
        .get

      log.info(s"Creating $number instances...")
      val instances = ec2.runInstances(req).getReservation.getInstances.asScala.toSeq

      val newMachines = instances.zip(names -- existMachines.map(_.name)).flatMap {
        case (instance, name) =>
          val id = instance.getInstanceId
          Try {
            log.info(s"Naming $name.")
            ec2.createTags(new CreateTagsRequest().withResources(id).withTags(new Tag("Name", name)))

            log.info(s"Getting address of $name.")
            val address = if (clusterConf.usePrivateIp) instance.getPrivateIpAddress else instance.getPublicDnsName
            assert(address != null && address != "", "Invalid address: " + address)

            Some(Machine(id, name, address))
          }.recover {
            case e: Exception =>
              log.warn(s"AWS API error on $name. Terminating instance ${id}.", e)
              ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(id))
              None
          }.recover {
            case e: Exception =>
              log.warn("Failed on terminating instance ${id}", e)
              None
          }.get
      }

      if (newMachines.size == number) {
        existMachines ++ newMachines
      } else if (attempts > 1) {
        fill(existMachines ++ newMachines, attempts - 1)
      } else {
        sys.error("Failed on creating enough instances.")
      }
    }

    //only try 3 times for now
    fill(Seq.empty, 3)
  }

  def destroyMachines(ids: Set[String]) = {
    ec2.terminateInstances(new TerminateInstancesRequest(ids.toSeq.asJava))
  }

  def getMachines() = {
    ec2.describeInstances().getReservations.asScala.flatMap(_.getInstances.asScala).toSeq
      .filter(_.getKeyName == clusterConf.keypair)
      .filter(_.getState.getName != "terminated")
      .map { i =>
        Machine(
          i.getInstanceId,
          i.getTags().asScala.find(_.getKey == "Name").map(_.getValue).getOrElse(""),
          if (clusterConf.usePrivateIp) i.getPrivateIpAddress else i.getPublicDnsName
        )
      }
  }
}
