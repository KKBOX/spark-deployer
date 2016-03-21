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
//import com.amazonaws.regions.Regions
//import com.amazonaws.services.ec2.AmazonEC2Client
//import com.amazonaws.services.ec2.model.{BlockDeviceMapping, CreateTagsRequest, EbsBlockDevice, RunInstancesRequest, Tag, TerminateInstancesRequest}
//import scala.util.{Failure, Success, Try}
import org.slf4s.Logging
import com.typesafe.config._
import net.ceedubs.ficus.Ficus._

import org.openstack4j.openstack.OSFactory
import org.openstack4j.api._
import org.openstack4j.model.compute._

import scala.collection.JavaConverters._


class OSMachines(config: Config) extends Machines with Logging {
  class OSConf(config: Config) extends ClusterConf(config) {
    val networkIds = config.as[Set[String]]("network-ids").toSeq
    val imageId = config.as[String]("image-id")

    val os_cacert = config.as[String]("os-cacert")
    val os_auth_url = config.as[String]("os-auth-url")
    val os_tenant_id = config.as[String]("os-tenant-id")
    val os_tenant_name = config.as[String]("os-tenant-name")
    val os_username = config.as[String]("os-username")
    //private val standardIn = System.console()
    //val os_password = standardIn.readPassword("Password> ").mkString
    val os_password = config.as[String]("os-password")
    // TODO: attach extra disk
    //val rootDevice = config.as[Option[String]]("root-device").getOrElse("/dev/xvda")
  }
  implicit val clusterConf = new OSConf(config)

  private val os: OSClient = OSFactory.builder()
    .endpoint(clusterConf.os_auth_url)
    .credentials(clusterConf.os_username, clusterConf.os_password)
    .tenantId(clusterConf.os_tenant_id)
    .tenantName(clusterConf.os_tenant_name)
    .useNonStrictSSLClient(true)
    .authenticate()

  def createMachines(machineType: MachineType, names: Set[String]): Seq[Machine] = {
    log.info(s"[OpenStack] Creating ${names.size} instances...")
    names.toSeq.map { name =>
      val flavor = os.compute().flavors().list().asScala.find(_.getName() == (machineType match {
        case Master => clusterConf.masterInstanceType
        case Worker => clusterConf.workerInstanceType
      })).get
      val image = os.compute().images().get(clusterConf.imageId)

      val bs = Builders.server()
      val serverCreate: ServerCreate = clusterConf.securityGroupIds
                              .map(ids => ids.toSeq.foldLeft(bs) {(bs, id) => bs.addSecurityGroup(id)}).get
                              .name(name)
                              .flavor(flavor)
                              .image(image)
                              .keypairName(clusterConf.keypair)
                              .networks(clusterConf.networkIds.toList.asJava)
                              .build();
      log.info(s"[OpenStack] Creating instance '${name}' ...")
      val server: Server = os.compute().servers().boot(serverCreate)
      retry { i =>
        log.info(s"[OpenStack] [${name}] Getting instance's address. Attempts: $i.")
        val thatServer = os.compute().servers().list().asScala.find(_.getId() == server.getId()).get
        val m = getMachine(thatServer)
        assert(m.id != null)
        assert(m.name != null)
        assert(m.address != null)
        m
      }
    }
  }

  private def getMachine(server: Server) = {
    val networkName = os.networking().network().get(clusterConf.networkIds.head).getName()
    val addr = server.getAddresses().getAddresses(networkName).asScala.head.getAddr()
    Machine(server.getId(), server.getName(), addr)
  }

  def getMachines(): Seq[Machine] = {
    os.compute().servers().list().asScala.map { s =>
      getMachine(s)
    }
  }

  def destroyMachines(ids: Set[String]): Unit = {
    val toKill = getMachines.filter { m => ids.contains(m.name) }
    log.info(s"[OpenStack] Terminating ${toKill.size} instances.")
    toKill.map { m =>
      os.compute().servers().delete(m.id)
    }
    log.info("[OpenStack] All instances are terminated.")
  }
}
