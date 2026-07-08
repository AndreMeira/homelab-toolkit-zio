package homelab.postgres.configuration


import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.FluentConfiguration

import scala.jdk.CollectionConverters.*


/**
 * Flyway migration settings. A plain case class the consuming service loads from config; the toolkit maps
 * it to a Flyway configuration via [[toFlywayConfig]].
 *
 * @param initSql SQL run on a fresh connection before migrating (e.g. a `SET`), or empty for none
 * @param locations where migrations live (e.g. `List("classpath:migrations/schema")`)
 * @param parameters extra Flyway configuration parameters
 * @param placeholders migration placeholder substitutions
 * @param allowClean whether `clean` is permitted (guarded off by default)
 */
final case class MigrationConfig(
  initSql: String,
  locations: List[String],
  parameters: Map[String, String] = Map.empty,
  placeholders: Map[String, String] = Map.empty,
  allowClean: Boolean = false,
):

  /**
   * Translate to a Flyway fluent configuration (without a datasource — [[homelab.postgres.PostgresMigration]]
   * attaches one).
   *
   * @return the Flyway configuration
   */
  def toFlywayConfig: FluentConfiguration =
    val base       = Flyway.configure().initSql(initSql).locations(locations*).cleanDisabled(!allowClean)
    val withParams = if parameters.nonEmpty then base.configuration(parameters.asJava) else base
    if placeholders.nonEmpty then withParams.placeholders(placeholders.asJava) else withParams
