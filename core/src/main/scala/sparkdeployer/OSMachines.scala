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
import org.openstack4j.core.transport.{Config => OSConfig}
import org.slf4s.Logging
import com.typesafe.config._
import net.ceedubs.ficus.Ficus._

import org.openstack4j.openstack.OSFactory
import org.openstack4j.api._
import org.openstack4j.model.compute._

import scala.collection.JavaConverters._

class OSMachines(config: Config) extends Machines with Logging {
  class OSConf(config: Config) extends ClusterConf(config) {
    val endpoint = config.as[String]("endpoint")
    val tenantId = config.as[String]("tenant-id")
    val tenantName = config.as[String]("tenant-name")

    private val standardIn = System.console()
    val username = standardIn.readLine("Enter OpenStack username> ")
    val password = standardIn.readPassword("Enter OpenStack password> ").mkString

    val masterFlavorName = config.as[String]("master.flavor-name")
    val workerFlavorName = config.as[String]("worker.flavor-name")
    
    val imageId = config.as[String]("image-id")
    val networkId = config.as[String]("network-id")

    // TODO: attach extra disk
    //val rootDevice = config.as[Option[String]]("root-device").getOrElse("/dev/xvda")
  }
  implicit val clusterConf = new OSConf(config)

  private def os(): OSClient = OSFactory.builder()
    .endpoint(clusterConf.endpoint)
    .credentials(clusterConf.username, clusterConf.password)
    .tenantId(clusterConf.tenantId)
    .tenantName(clusterConf.tenantName)
    .withConfig(OSConfig.newConfig().withSSLVerificationDisabled())
    .authenticate()

  def createMachines(machineType: MachineType, names: Set[String]): Seq[Machine] = {
    log.info(s"[OpenStack] Creating ${names.size} instances...")

    val flavor = os().compute().flavors().list().asScala.find(_.getName() == (machineType match {
      case Master => clusterConf.masterFlavorName
      case Worker => clusterConf.workerFlavorName
    })).get
    
    val image = os().compute().images().get(clusterConf.imageId)
    assert(image != null)

    names.toSeq.map { name =>
      val serverCreate = Some(Builders.server())
        .map { bs =>
          clusterConf.securityGroupIds.map(_.toSeq).getOrElse(Seq.empty)
            .foldLeft(bs)((bs, id) => bs.addSecurityGroup(id))
        }
        .get
        .name(name)
        .flavor(flavor)
        .image(image)
        .keypairName(clusterConf.keypair)
        .networks(Seq(clusterConf.networkId).asJava)
        .build()
      log.info(s"[OpenStack] Creating instance '${name}' ...")
      val server: Server = os().compute().servers().boot(serverCreate)
      retry { i =>
        log.info(s"[OpenStack] [${name}] Getting instance's address. Attempts: $i.")
        val thatServer = os().compute().servers().list().asScala.find(_.getId() == server.getId()).get
        val m = getMachine(thatServer)
        assert(m.id != null)
        assert(m.name != null)
        assert(m.address != null)
        m
      }
    }
  }

  private def getMachine(server: Server) = {
    val networkName = os().networking().network().get(clusterConf.networkId).getName()
    val addr = server.getAddresses().getAddresses(networkName).asScala.head.getAddr()
    Machine(server.getId(), server.getName(), addr)
  }

  def getMachines(): Seq[Machine] = {
    os().compute().servers().list().asScala.map { s =>
      getMachine(s)
    }
  }

  def destroyMachines(ids: Set[String]): Unit = {
    val toKill = getMachines.filter { m => ids.contains(m.id) }
    log.info(s"[OpenStack] Terminating ${toKill.size} instances.")
    toKill.map { m =>
      os().compute().servers().delete(m.id)
    }
    retry { i =>
      log.info(s"[OpenStack] Checking status. Attempts: ${i}.")
      val liveMachines = getMachines.filter { m => ids.contains(m.id) }
      assert(liveMachines.size == 0)
    }
    log.info("[OpenStack] All instances are terminated.")
  }
}
