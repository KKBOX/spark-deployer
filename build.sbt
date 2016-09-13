scalaVersion := "2.10.6"

lazy val commonSettings = Seq(
  organization := "net.pishen",
  version := "3.0.2",
  scalaVersion := "2.10.6",
  licenses += ("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html")),
  homepage := Some(url("https://github.com/pishen/spark-deployer")),
  pomExtra := (
    <scm>
      <url>https://github.com/pishen/spark-deployer.git</url>
      <connection>scm:git:git@github.com:pishen/spark-deployer.git</connection>
    </scm>
    <developers>
      <developer>
        <id>pishen</id>
        <name>Pishen Tsai</name>
      </developer>
    </developers>
  )
)

//publish core before other sub-projects
lazy val core = (project in file("core"))
  .settings(commonSettings: _*)
  .settings(
    name := "spark-deployer-core",
    crossScalaVersions := Seq("2.10.6", "2.11.8"),
    libraryDependencies ++= {
      Seq(
        "com.github.pathikrit" %% "better-files" % "2.14.0",
        "com.typesafe.play" %% "play-json" % "2.4.8",
        "com.amazonaws" % "aws-java-sdk-ec2" % "1.11.23",
        "org.scalaj" %% "scalaj-http" % "2.3.0",
        "org.slf4s" %% "slf4s-api" % "1.7.12",
        "com.typesafe" % "config" % "1.3.0"
      ) ++ {
        CrossVersion.partialVersion(scalaVersion.value) match {
          case Some((2, scalaMajor)) if scalaMajor >= 11 =>
            Some("org.scala-lang.modules" %% "scala-xml" % "1.0.3")
          case _ =>
            None
        }
      }
    }
  )

lazy val plugin = (project in file("plugin"))
  .settings(commonSettings: _*)
  .settings(
    sbtPlugin := true,
    name := "spark-deployer-sbt",
    addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3"),
    publishMavenStyle := false,
    libraryDependencies ++= Seq(
      "com.github.eirslett" %% "sbt-slf4j" % "0.1",
      "com.github.pathikrit" %% "better-files" % "2.14.0"
    ),
    //test
    ScriptedPlugin.scriptedSettings,
    scriptedLaunchOpts ++= Seq("-Xmx1024M", "-Dplugin.version=" + version.value),
    scriptedBufferLog := false
  )
  .dependsOn(core)
