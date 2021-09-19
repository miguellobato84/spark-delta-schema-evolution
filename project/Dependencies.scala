import sbt._

object Dependencies {
  lazy val sparkVersion = "3.1.1"
  lazy val testVersion  = "3.2.2"

  lazy val scalaTest         = "org.scalatest"              %% "scalatest"                   % testVersion
  lazy val scalaTestFlatSpec = "org.scalatest"              %% "scalatest-flatspec"          % testVersion
  lazy val sparkCore         = "org.apache.spark"           %% "spark-core"                  % sparkVersion
  lazy val sparkSQL          = "org.apache.spark"           %% "spark-sql"                   % sparkVersion
  lazy val sparkHive         = "org.apache.spark"           %% "spark-hive"                  % sparkVersion
  lazy val deltaLake         = "io.delta"                   %% "delta-core"                  % "1.0.0"
  lazy val scalaLogging      = "com.typesafe.scala-logging" %% "scala-logging"               % "3.9.4"
}
