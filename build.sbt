name := "IdealPageRank"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.4.4"
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion
)

