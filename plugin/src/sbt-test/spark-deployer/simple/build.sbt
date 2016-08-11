lazy val root = (project in file(".")).settings(
  scalaVersion := "2.11.8",
  libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
)
