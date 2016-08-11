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

object Machines {
  def createMachines(names: Set[String], machineConf: MachineConf)(implicit conf: ClusterConf): Seq[Machine] = {
    
  }
  
  def destroyMachines(ids: Set[String]): Unit
  def getMachines(): Seq[Machine]
}

case class Machine(id: String, name: String, address: String)
