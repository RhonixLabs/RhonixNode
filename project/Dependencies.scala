import sbt.*

object Dependencies {
  // Core dependencies
  val catsCore   = "org.typelevel" %% "cats-core"   % "2.9.0" // cross CrossVersion.for3Use2_13
  val catsEffect = "org.typelevel" %% "cats-effect" % "3.5.0" // cross CrossVersion.for3Use2_13
  val fs2Core    = "co.fs2"        %% "fs2-core"    % "3.7.0" // cross CrossVersion.for3Use2_13

  // Network communication
  val grpc      = "io.grpc" % "grpc-core"  % "1.53.0"
  val grpcNetty = "io.grpc" % "grpc-netty" % "1.53.0"

  // LEGACY dependencies of imported projects
  val protobuf = "com.google.protobuf" % "protobuf-java" % "3.22.2"

  // Testing frameworks
  val scalatest    = "org.scalatest" %% "scalatest" % "3.2.15" % Test // cross CrossVersion.for3Use2_13
  val scalatest_ce =
    "org.typelevel" %% "cats-effect-testing-scalatest" % "1.4.0" % Test // cross CrossVersion.for3Use2_13

  val tests = Seq(scalatest, scalatest_ce)

  // Database
  val postgresql                 = "org.postgresql" % "postgresql" % "42.6.0"
  val squeryl                    = "org.squeryl"   %% "squeryl"    % "0.9.18"
  val liquibase4s: Seq[ModuleID] = Seq(
    "io.github.liquibase4s" %% "liquibase4s-core"        % "1.0.0",
    // if you want to use cats.effect.IO
    "io.github.liquibase4s" %% "liquibase4s-cats-effect" % "1.0.0",
  )

  val dbLibs: Seq[ModuleID] = Seq(postgresql, squeryl) ++ liquibase4s
}
