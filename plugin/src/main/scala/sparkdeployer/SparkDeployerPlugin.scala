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
import sbinary.DefaultProtocol.StringFormat
import Cache.seqFormat
import sbt.Defaults.runMainParser
import sbt.complete.DefaultParsers._
import sbt.Def.{spaceDelimited, macroValueIT}
import sbt.Keys._
import sbtassembly.AssemblyKeys._

object SparkDeployerPlugin extends AutoPlugin {

  object autoImport {
    lazy val sparkConfig = settingKey[Option[(String, ClusterConf)]]("spark-deployer's config.")

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
        (success("default") ~ success(None)) |
          (Space ~> token(NotSpace, "<new-config-name>") ~
            (if (configNames.isEmpty) success(None) else (" from " ~> oneOf(configNames.map(literal))).?))
      } {
        case (state, (newConfigName, baseConfigNameOpt)) =>
          StaticLoggerBinder.sbtLogger = state.log

          val extracted = Project.extract(state)
          import extracted._

          val suggestedClusterName = name in currentRef get structure.data
          val suggestedSparkVersion = (libraryDependencies in currentRef get structure.data)
            .flatMap(libs => libs.find(_.name == "spark-core").map(_.revision))

          val templateConfOpt = baseConfigNameOpt.map { name =>
            ClusterConf.load(confDir.pathAsString + "/" + name + ".json")
          }
          val clusterConf = ClusterConf.build(templateConfOpt, suggestedClusterName, suggestedSparkVersion)

          clusterConf.save(confDir.pathAsString + "/" + newConfigName + ".json")
          Project.extract(state).append(Seq(sparkConfig := Some(newConfigName -> clusterConf)), state)
      },
      Command("sparkChangeConfig") { _ =>
        Space ~> NotSpace.examples(configNames: _*)
      } { (state, configName) =>
        StaticLoggerBinder.sbtLogger = state.log

        val configFile = (confDir / s"${configName}.json")
        if (configFile.exists) {
          val clusterConf = ClusterConf.load(configFile.pathAsString)
          Project.extract(state).append(Seq(sparkConfig := Some(configName -> clusterConf)), state)
        } else {
          state.log.error(s"Config ${configName} does not exist.")
          state
        }
      }
    ),
    sparkCreateCluster := {
      val log = streams.value.log
      StaticLoggerBinder.sbtLogger = log
      
      val workers = (Space ~> NatBasic).parsed
      
      sparkConfig.value match {
        case None =>
          log.error("You don't have default config, use `sparkBuildConfig` to build one.")
        case Some((_, clusterConf)) =>
          new SparkDeployer()(clusterConf).createCluster(workers)
      }
    },
    sparkAddWorkers := {
      val log = streams.value.log
      StaticLoggerBinder.sbtLogger = log
      
      val workers = (Space ~> NatBasic).parsed
      
      sparkConfig.value match {
        case None =>
          log.error("You don't have default config, use `sparkBuildConfig` to build one.")
        case Some((_, clusterConf)) =>
          new SparkDeployer()(clusterConf).addWorkers(workers)
      }
    },
    sparkRemoveWorkers := {
      val log = streams.value.log
      StaticLoggerBinder.sbtLogger = log
      
      val workers = (Space ~> NatBasic).parsed
      
      sparkConfig.value match {
        case None =>
          log.error("You don't have default config, use `sparkBuildConfig` to build one.")
        case Some((_, clusterConf)) =>
          new SparkDeployer()(clusterConf).removeWorkers(workers)
      }
    },
    sparkDestroyCluster := {
      val log = streams.value.log
      StaticLoggerBinder.sbtLogger = log
      
      sparkConfig.value match {
        case None =>
          log.error("You don't have default config, use `sparkBuildConfig` to build one.")
        case Some((_, clusterConf)) =>
          new SparkDeployer()(clusterConf).destroyCluster()
      }
    },
    sparkShowMachines := {
      val log = streams.value.log
      StaticLoggerBinder.sbtLogger = log
      
      sparkConfig.value match {
        case None =>
          log.error("You don't have default config, use `sparkBuildConfig` to build one.")
        case Some((_, clusterConf)) =>
          new SparkDeployer()(clusterConf).showMachines()
      }
    },
    sparkSubmit := {
      val log = streams.value.log
      StaticLoggerBinder.sbtLogger = log
      
      val args = spaceDelimited("<args-for-your-job>").parsed
      
      sparkConfig.value match {
        case None =>
          log.error("You don't have default config, use `sparkBuildConfig` to build one.")
        case Some((_, clusterConf)) =>
          (mainClass in Compile).value match {
            case None =>
              log.error("I can't determine a main class for you, please use `sparkSubmitMain` if you have multiple main classes.")
            case Some(mainClass) =>
              val jar = assembly.value
              new SparkDeployer()(clusterConf).submit(jar, mainClass, args)
          }
      }
    },
    //copied from https://github.com/sbt/sbt/blob/v0.13.12/main/src/main/scala/sbt/Defaults.scala#L734
    sparkSubmitMain <<= {
      val parser = loadForParser(discoveredMainClasses in Compile)((s, names) => runMainParser(s, names getOrElse Nil))
      Def.inputTask {
        val log = streams.value.log
        StaticLoggerBinder.sbtLogger = log
        
        val (mainClass, args) = parser.parsed
        sparkConfig.value match {
          case None =>
            log.error("You don't have default config, use `sparkBuildConfig` to build one.")
          case Some((_, clusterConf)) =>
            val jar = assembly.value
            new SparkDeployer()(clusterConf).submit(jar, mainClass, args)
        }
      }
    }
  )
}

