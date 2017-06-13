name := "ppiv"

version := "0.0.1"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.1"
resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
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
  "com.github.nscala-time" % "nscala-time_2.11" % "2.16.0",
  "org.elasticsearch" %% "elasticsearch-spark" % "2.2.0",
  "com.databricks" % "spark-csv_2.10" % "1.2.0"
)




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