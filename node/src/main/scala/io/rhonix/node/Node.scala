package io.rhonix.node

import io.github.liquibase4s.{Liquibase, LiquibaseConfig}

class Node {}

object Node {
  def main(args: Array[String]): Unit =
    applyDBMigrations()

  private def applyDBMigrations(): Unit = {
    val config: LiquibaseConfig = LiquibaseConfig(
      url = "jdbc:postgresql://localhost:5432/rhonixnode",
      user = "dbuser",
      password = "letmein",
      driver = "org.postgresql.Driver",
      changelog = "db/changelog.yaml",
    )
    Liquibase(config).migrate()
  }
}
