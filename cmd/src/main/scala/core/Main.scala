package core

import java.io.File

import sparkdeployer.ClusterConf
import sparkdeployer.SparkDeployer

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
    }
  }
}