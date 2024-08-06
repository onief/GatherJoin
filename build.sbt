scalaVersion := "2.12.18"

name := "BachelorThesis"
organization := "unimainz"
version := "1.0"

// Main Dependencies
libraryDependencies ++= Seq(
  // TODO: add "provided" to the spark dependencies when packaging for spark cluster
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql"  % "3.5.1",
  "edu.ucar" % "cdm" % "4.5.5"
)

// Test Dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.19" % "test"
)

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions ++= Seq("-target:jvm-1.8")

Compile / run / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

// Assembly Settings
assembly / assemblyJarName := "gather-join-1.0.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / assemblyExcludedJars := {
  val cp = (Test / fullClasspath).value
  cp filter { file =>
    val f = file.data.getName
    f.contains("scalatest") || f.contains("junit")
  }
}