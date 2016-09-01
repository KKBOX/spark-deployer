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
    lazy val configName = settingKey[Option[String]]("spark-deployer's config name.")
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
  
  override lazy val projectSettings = Seq(
    configName := {
      if (File("conf/default.deployer.conf").exists) Some("default") else None
    },
    shellPrompt := { state =>
      Project.extract(state).get(configName).map("[\u001B[93m" + _ + "\u001B[0m]> ").getOrElse("> ")
    },
    commands ++= Seq(
      Command("sparkBuildConfig")(_ => token(Space ~> NotQuoted) | success("default")) { (state, newConfigName) =>
        StaticLoggerBinder.sbtLogger = state.log
        
        val extracted = Project.extract(state)
        import extracted._
        
        File("conf").createIfNotExists(true)
        
        val configFile = File(s"conf/${newConfigName}.deployer.conf")
        if (configFile.exists) {
          state.log.error(s"config ${newConfigName} already exists, please use `sparkBuildConfig <new-config-name>` or delete the config file in conf/")
          state
        } else {
          val suggestedClusterName = name in currentRef get structure.data
          val suggestedSparkVersion = (libraryDependencies in currentRef get structure.data)
            .flatMap(libs => libs.find(_.name == "spark-core").map(_.revision))
          
          val clusterConf = if (newConfigName == "default"){
            ClusterConf.build(None, suggestedClusterName, suggestedSparkVersion)
          } else {
            val configNames = File("conf").children.toSeq.map(_.name.split("\\.").dropRight(2).mkString("."))
            val templateConf = if (configNames.nonEmpty) {
              println("There are existing configs in your conf/ directory, you can optionally choose one as a template:")
              println(configNames.mkString("\t"))
              val res = stdin.readLine("template config [None]: ").trim
              if (res == "") None else Some(ClusterConf.load(s"conf/${res}.deployer.conf"))
            } else None
            
            ClusterConf.build(templateConf, suggestedClusterName, suggestedSparkVersion)
          }
          
          clusterConf.save(s"conf/${newConfigName}.deployer.conf")
          Project.extract(state).append(Seq(configName := Some(newConfigName)), state)
        }
      }
    )
  )
}

