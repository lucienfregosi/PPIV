name := "ppiv"

version := "0.0.2-SNAPSHOT"


scalaVersion := "2.10.6"

val sparkVersion = "1.6.1"
resolvers ++= Seq(
  "apache-snapshots" at "https://oss.sonatype.org/content/repositories/releases/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.commons" % "commons-csv" % "1.1",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "com.typesafe" % "config" % "1.2.1",
  "org.joda" % "joda-convert" % "1.8",
  "log4j" % "log4j" % "1.2.14",
  "com.github.nscala-time" % "nscala-time_2.10" % "2.16.0",
  "org.elasticsearch" %% "elasticsearch-spark" % "2.2.0",
  "com.databricks" % "spark-csv_2.10" % "1.2.0",
  "org.specs2" % "specs2-scalacheck_2.10" % "3.9.0" % "test",
  "org.specs2" % "specs2-core_2.10" % "3.9.0" % "test",
  "junit" % "junit" % "4.12" % "test",
  "org.scoverage" % "scalac-scoverage-plugin_2.10" % "1.3.0" % "provided",
  "org.apache.commons" % "commons-csv" % "1.4",
  "com.univocity" % "univocity-parsers" % "2.4.0"

)

publishTo := {
  val nexus = "http://rogno.socrate.vsct.fr:60090/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "content/repositories/releases")
}

credentials += Credentials("Nexus Repository Manager",
                           "rogno.socrate.vsct.fr",
                           "deployment",
                           "deployment123")


test in assembly := {}


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
      case _ => MergeStrategy.discard
    }
  case "application.conf"                            => MergeStrategy.concat
  case _                                => MergeStrategy.first
}

enablePlugins(SonarRunnerPlugin)
coverageEnabled.in(ThisBuild ,Test, test) := true
publishMavenStyle := true
