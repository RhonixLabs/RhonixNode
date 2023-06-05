import Dependencies._

lazy val projectSettings = Seq(organization := "io.rhonix", scalaVersion := "2.13.10", version := "0.1.0-SNAPSHOT")

lazy val commonSettings = projectSettings

// Tools to build graphs
lazy val graphs = (project in file("graphs"))
  .settings(commonSettings: _*)
  .settings(
    version := "0.1",
    libraryDependencies ++= Seq(catsCore, catsEffect, fs2Core) ++ tests
  )

// Consensus
lazy val weaver = (project in file("weaver"))
  .settings(commonSettings: _*)
  .settings(
    version := "0.1",
    libraryDependencies ++= Seq(catsCore, catsEffect, fs2Core) ++ tests
  )
  .dependsOn(graphs % "test", sdk)

// Node logic
lazy val dproc = (project in file("dproc"))
  .settings(commonSettings: _*)
  .settings(
    version := "0.1",
    libraryDependencies ++= Seq(catsCore, catsEffect, fs2Core) ++ tests
  )
  .dependsOn(weaver, graphs % "test")

// Node implementation
lazy val node = (project in file("node"))
  .settings(commonSettings: _*)
  .settings(
    version := "0.1",
    libraryDependencies ++= Seq(catsCore, catsEffect, protobuf, grpc, grpcNetty) ++ tests
  )
  .dependsOn(dproc)

// SDK
lazy val sdk = (project in file("sdk"))
  .settings(commonSettings: _*)
  .settings(
    version := "0.1",
    libraryDependencies ++= Seq(catsCore, catsEffect, fs2Core, grpc, grpcNetty) ++ tests
  )
  .dependsOn(graphs % "test")
