import sbtassembly.Plugin._
import sbtassembly.Plugin.MergeStrategy
import AssemblyKeys._

assemblySettings

name := "HbaseWQ"

version := "1.0"

scalaVersion := "2.10.5"

mainClass in (Compile, run) := Some("org.komono.sqlanalytics.Main")

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

resolvers += Resolver.sonatypeRepo("public")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.apache.hbase" % "hbase-client" % "1.1.2",
  "org.apache.hbase" % "hbase-common" % "1.1.2",
  "org.apache.hadoop" % "hadoop-common" % "2.7.1"
)

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", "MANIFEST.MF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}
