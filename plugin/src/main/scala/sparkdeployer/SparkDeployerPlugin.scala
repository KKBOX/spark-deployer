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

import better.files._
import org.slf4j.impl.StaticLoggerBinder
import sbt._
import sbt.complete.DefaultParsers._
import sbt.Def.{spaceDelimited, macroValueIT}
import sbt.Keys._
import sbtassembly.AssemblyKeys._

object SparkDeployerPlugin extends AutoPlugin {

  object autoImport {
    lazy val sparkConfig = settingKey[Option[(String, ClusterConf)]]("spark-deployer's config.")
    //lazy val sparkBuildConfig = inputKey[Unit]("Build a spark-deployer config.")
    //lazy val sparkChangeConfig = inputKey[Unit]("Change current spark-deployer config.")

    lazy val sparkCreateCluster = inputKey[Unit]("Create a cluster.")
    lazy val sparkAddWorkers = inputKey[Unit]("Add workers.")
    lazy val sparkRemoveWorkers = inputKey[Unit]("Remove workers.")
    lazy val sparkDestroyCluster = taskKey[Unit]("Destroy the cluster.")

    lazy val sparkShowMachines = taskKey[Unit]("Show the information of the machines.")

    lazy val sparkSubmit = inputKey[Unit]("Submit the job.")
    lazy val sparkSubmitMain = inputKey[Unit]("Submit the job with specified main class.")
  }
  import autoImport._
  override def trigger = allRequirements

  val stdin = System.console()
  lazy val confDir = File("conf").createIfNotExists(true)
  def configNames = confDir.children.toSeq.map(_.name.split("\\.").init.mkString("."))

  override lazy val projectSettings = Seq(
    sparkConfig := {
      val defaultConfFile = confDir / "default.json"
      if (defaultConfFile.exists) Some("default" -> ClusterConf.load(defaultConfFile.pathAsString)) else None
    },
    shellPrompt := { state =>
      Project.extract(state).get(sparkConfig).map("[\u001B[93m" + _._1 + "\u001B[0m]> ").getOrElse("> ")
    },
    commands ++= Seq(
      Command("sparkBuildConfig") { _ =>
        success("default" -> None) |
          (Space ~> token(NotSpace, "<new-config-name>") ~
            (if (configNames.isEmpty) success(None) else (" from " ~> oneOf(configNames.map(literal))).?))
      } {
        case (state, (newConfigName, baseConfigNameOpt)) =>
          StaticLoggerBinder.sbtLogger = state.log

          val extracted = Project.extract(state)
          import extracted._

          val configFile = confDir / s"${newConfigName}.json"
          if (configFile.exists) {
            state.log.error(s"config ${newConfigName} already exists, please use `sparkBuildConfig <new-config-name>` or delete the config file in conf/")
            state
          } else {
            val suggestedClusterName = name in currentRef get structure.data
            val suggestedSparkVersion = (libraryDependencies in currentRef get structure.data)
              .flatMap(libs => libs.find(_.name == "spark-core").map(_.revision))

            val clusterConf = if (newConfigName == "default") {
              ClusterConf.build(None, suggestedClusterName, suggestedSparkVersion)
            } else {
              val templateConfOpt = baseConfigNameOpt.map { name =>
                ClusterConf.load(confDir.pathAsString + "/" + name + ".json")
              }
              ClusterConf.build(templateConfOpt, suggestedClusterName, suggestedSparkVersion)
            }

            clusterConf.save(confDir.pathAsString + "/" + newConfigName + ".json")
            Project.extract(state).append(Seq(sparkConfig := Some(newConfigName -> clusterConf)), state)
          }
      }
    )
  )
}

