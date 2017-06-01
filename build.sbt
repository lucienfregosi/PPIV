name := "sbt-scala-sample"

version := "1.0"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.2"
resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.typesafe" % "config" % "1.2.1",
  "org.joda" % "joda-convert" % "1.8",
  "log4j" % "log4j" % "1.2.14",
  "com.github.nscala-time" % "nscala-time_2.10" % "2.16.0",
  "org.elasticsearch" % "elasticsearch-spark-20_2.10" % "2.4.4" % "compile"

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