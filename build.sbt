import Dependencies._

ThisBuild / scalaVersion := "2.12.13"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.miguellobato"
ThisBuild / organizationName := "miguellobato"

lazy val root = (project in file("."))
  .settings(
    name := "spark-delta-schema-evolution",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += sparkCore,
    libraryDependencies += sparkSQL,
    libraryDependencies += sparkHive,
    libraryDependencies += deltaLake,
    libraryDependencies += scalaLogging,
    libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test"
  )
