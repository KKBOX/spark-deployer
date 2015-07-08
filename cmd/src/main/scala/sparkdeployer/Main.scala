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

object Main {
  def main(args: Array[String]) {
    val sparkDeployer = new SparkDeployer(ClusterConf.fromFile("spark-deployer.conf"))

    args.head match {
      case "--create-cluster" =>
        val num = args.tail.head.toInt
        sparkDeployer.createCluster(num)

      case "--submit-job" =>
        val jar = new File(args.tail.head)
        require(jar.exists)
        sparkDeployer.submitJob(jar, args.drop(2))

      case "--destroy-cluster" =>
        sparkDeployer.destroyCluster()

      case "--add-workers" =>
        val num = args.tail.head.toInt
        sparkDeployer.addWorkers(num)
        
      case "--show-machines" =>
        sparkDeployer.showMachines()
    }
  }
}
