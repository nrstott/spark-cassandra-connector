package org.apache.spark.sql.cassandra


import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{ResolvedDataSource, LogicalRelation}
import org.apache.spark.sql.types.{StructType, DataType}

import com.datastax.driver.core.{Row, PreparedStatement}
import com.datastax.spark.connector.cql.{Schema, CassandraConnector}


trait MetaStore {

  /**
   * Get a table from metastore. If it's not found in metastore, then look
   * up Cassandra tables to get the source table.
   *
   */
  def getTable(tableIdent: TableIdent) : LogicalPlan

  /** Get a table from metastore. If it's not found in metastore, return None */
  def getTableFromMetastore(tableIdent: TableIdent) : Option[LogicalPlan]

  /**
   * Get all table names for a keyspace. If keyspace is empty, get all tables from
   * all keyspaces.
   */
  def getAllTables(keyspace: Option[String], cluster: Option[String] = None) : Seq[(String, Boolean)]


  /** Return all database names */
  def getAllDatabases(cluster: Option[String] = None) : Seq[String]

  /** Return all cluster names */
  def getAllClusters() : Seq[String]

  /** Only Store customized tables meta data in metastore */
  def storeTable(
      tableIdentifier: TableIdent,
      source: String,
      schema: Option[StructType],
      options: Map[String, String]) : Unit

  /** create a database in metastore */
  def storeDatabase(database: String, cluster: Option[String]) : Unit

  /** create a cluster in metastore */
  def storeCluster(cluster: String) : Unit

  /** Remove table from metastore */
  def removeTable(tableIdent: TableIdent) : Unit

  /** Remove a database from metastore */
  def removeDatabase(database: String, cluster: Option[String]) : Unit

  /** Remove a cluster from metastore */
  def removeCluster(cluster: String) : Unit

  /** Remove all tables from metastore */
  def removeAllTables() : Unit

  /** Create metastore keyspace and table in Cassandra */
  def init() : Unit

}

/**
 * Store only customized tables or other data source tables. Cassandra data source tables
 * are directly lookup from Cassandra tables
 */
class DataSourceMetaStore(sqlContext: SQLContext) extends MetaStore with Logging {

  import DataSourceMetaStore._
  import CassandraDefaultSource._

  private val metaStoreConn = new CassandraConnector(sqlContext.getCassandraConnConf(getMetaStoreCluster()))

  private val CreateMetaStoreKeyspaceQuery =
    s"""
      |CREATE KEYSPACE IF NOT EXISTS ${getMetaStoreKeyspace()}
      | WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """.stripMargin.replaceAll("\n", " ")

  private val CreateMetaStoreTableQuery =
    s"""
      |CREATE TABLE IF NOT EXISTS ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
      | (cluster_name text,
      |  keyspace_name text,
      |  table_name text,
      |  source_provider text,
      |  schema_json text,
      |  options map<text, text>,
      |  PRIMARY KEY (cluster_name, keyspace_name, table_name))
    """.stripMargin.replaceAll("\n", " ")

  private val InsertIntoMetaStoreQuery =
    s"""
      |INSERT INTO ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
      | (cluster_name, keyspace_name, table_name, source_provider, schema_json, options)
      | values (?, ?, ?, ?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")

  private val InsertIntoMetaStoreWithoutSchemaQuery =
    s"""
      |INSERT INTO ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
      | (cluster_name, keyspace_name, table_name, source_provider, options) values (?, ?, ?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")


  override def getTable(tableIdent: TableIdent): LogicalPlan = {
      getTableFromMetastore(tableIdent).getOrElse(getTableMayThrowException(tableIdent))
  }

  override def getAllTables(keyspace: Option[String], cluster: Option[String] = None): Seq[(String, Boolean)] = {
    val clusterName = cluster.getOrElse(sqlContext.getDefaultCluster)
    val selectQuery =
      s"""
      |SELECT table_name, keyspace_name
      |From ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
      |WHERE cluster_name = '$clusterName'
    """.stripMargin.replaceAll("\n", " ")
    val names = ListBuffer[(String, Boolean)]()
    // Add source tables from metastore
    metaStoreConn.withSessionDo {
      session =>
        val result = session.execute(selectQuery).iterator()
        while (result.hasNext) {
          val row: Row = result.next()
          val tableName = row.getString(0)
          if (tableName != TempDatabaseOrTableName) {
            val ks = row.getString(1)
            if (keyspace.nonEmpty) {
              if (ks == keyspace.get)
                names += ((tableName, false))
            } else {
              if (ks != TempDatabaseOrTableName)
                names += ((tableName, false))
            }
          }
        }
        names
    }

    // Add source tables from Cassandra tables
    val conn = new CassandraConnector(sqlContext.getCassandraConnConf(clusterName))
    if (keyspace.nonEmpty) {
      val ksDef = Schema.fromCassandra(conn).keyspaceByName.get(keyspace.get)
      names ++= ksDef.map(_.tableByName.keySet).getOrElse(Set.empty).map((name => (name, false)))
    }
     names.toList
  }

  override def getAllDatabases(cluster: Option[String] = None): Seq[String] = {
    val clusterName = cluster.getOrElse(sqlContext.getDefaultCluster)
    val selectQuery =
      s"""
      |SELECT keyspace_name
      |From ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
      |WHERE cluster_name = '$clusterName'
    """.stripMargin.replaceAll("\n", " ")
    val databaseNames = getAllDatabasesFromMetastore(cluster).toSet

    // Add source tables from Cassandra tables
    val conn = new CassandraConnector(sqlContext.getCassandraConnConf(clusterName))
    val keyspaces = Schema.fromCassandra(conn).keyspaceByName.keySet
    (databaseNames ++ keyspaces -- SystemKeyspaces).toSeq
  }

  def getAllDatabasesFromMetastore(cluster: Option[String] = None): Seq[String] = {
    val clusterName = cluster.getOrElse(sqlContext.getDefaultCluster)
    val selectQuery =
      s"""
      |SELECT keyspace_name
      |From ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
      |WHERE cluster_name = '$clusterName'
    """.stripMargin.replaceAll("\n", " ")
    val names = ListBuffer[String]()
    // Add source tables from metastore
    metaStoreConn.withSessionDo {
      session =>
        val result = session.execute(selectQuery).iterator()
        while (result.hasNext) {
          val row: Row = result.next()
          val keyspaceName = row.getString(0)
          if (keyspaceName != TempDatabaseOrTableName) {
            names += keyspaceName
          }
        }
        names
    }
    names.toList
  }

  override def getAllClusters(): Seq[String] = {
    val names = getAllClustersFromMetastore
    (names ++ Seq(sqlContext.getDefaultCluster)).distinct
  }

  def getAllClustersFromMetastore: Seq[String] = {
    val selectQuery =
      s"""
      |SELECT cluster_name
      |From ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
    """.stripMargin.replaceAll("\n", " ")
    val names = ListBuffer[String]()
    // Add source tables from metastore
    metaStoreConn.withSessionDo {
      session =>
        val result = session.execute(selectQuery).iterator()
        while (result.hasNext) {
          val row: Row = result.next()
          val clusterName = row.getString(0)
          names += clusterName
        }
        names
    }

    names.distinct.toList
  }

  /** Store a tale with the creation meta data */
  override def storeTable(
      tableIdent: TableIdent,
      source: String,
      schema: Option[StructType],
      options: Map[String, String]): Unit = {
    import collection.JavaConversions._

    val cluster = tableIdent.cluster.getOrElse(sqlContext.getDefaultCluster)
    if (schema.nonEmpty) {
      metaStoreConn.withSessionDo {
        session =>
          val preparedStatement: PreparedStatement = session.prepare(InsertIntoMetaStoreQuery)
          session.execute(
            preparedStatement.bind(
              cluster,
              tableIdent.keyspace,
              tableIdent.table,
              source,
              schema.get.json,
              mapAsJavaMap(options)))
      }
    } else {
      metaStoreConn.withSessionDo {
        session =>
          val preparedStatement: PreparedStatement = session.prepare(InsertIntoMetaStoreWithoutSchemaQuery)
          session.execute(
            preparedStatement.bind(
              cluster,
              tableIdent.keyspace,
              tableIdent.table,
              source,
              mapAsJavaMap(options)))
      }
    }

    val tempTableIdent = TableIdent(TempDatabaseOrTableName, TempDatabaseOrTableName, Option(cluster))
    removeTable(tempTableIdent)
  }

  override def storeDatabase(database: String, cluster: Option[String]) : Unit = {
    val databaseNames = getAllDatabasesFromMetastore(cluster).toSet
    if (databaseNames.contains(database))
      return

    val insertQuery =
      s"""
      |INSERT INTO ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
      | (cluster_name, keyspace_name, table_name)
      | values (?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")

    val clusterName = cluster.getOrElse(sqlContext.getDefaultCluster)
    metaStoreConn.withSessionDo {
      session =>
        val preparedStatement: PreparedStatement = session.prepare(insertQuery)
        session.execute(
          preparedStatement.bind(clusterName, database, TempDatabaseOrTableName))
    }
  }

  override def storeCluster(cluster: String) : Unit = {
    val clusterNames = getAllClustersFromMetastore.toSet
    if (clusterNames.contains(cluster))
      return

    val insertQuery =
      s"""
      |INSERT INTO ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
      | (cluster_name, keyspace_name, table_name)
      | values (?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")

    metaStoreConn.withSessionDo {
      session =>
        val preparedStatement: PreparedStatement = session.prepare(insertQuery)
        session.execute(
          preparedStatement.bind(cluster, TempDatabaseOrTableName, TempDatabaseOrTableName))
    }
  }

  override def removeTable(tableIdent: TableIdent) : Unit = {
    val deleteQuery =
      s"""
        |DELETE FROM ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
        |WHERE cluster_name = '${tableIdent.cluster.getOrElse(sqlContext.getDefaultCluster)}'
        | AND keyspace_name = '${tableIdent.keyspace}'
        | AND table_name = '${tableIdent.table}'
      """.stripMargin.replaceAll("\n", " ")

    metaStoreConn.withSessionDo {
      session => session.execute(deleteQuery)
    }
  }

  override def removeDatabase(database: String, cluster: Option[String]) : Unit = ???

  override def removeCluster(cluster: String) : Unit = ???

  override def removeAllTables() : Unit = {
    metaStoreConn.withSessionDo {
      session => session.execute(s"TRUNCATE ${getMetaStoreKeyspace()}.${getMetaStoreTable()}")
    }
  }

  override def init() : Unit = {
    metaStoreConn.withSessionDo {
      session =>
        session.execute(CreateMetaStoreKeyspaceQuery)
        session.execute(CreateMetaStoreTableQuery)
    }
  }

  /** Look up source table from metastore */
  def getTableFromMetastore(tableIdent: TableIdent): Option[LogicalPlan] = {
    val selectQuery =
      s"""
        |SELECT source_provider, schema_json, options
        |FROM ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
        |WHERE cluster_name = '${tableIdent.cluster.getOrElse(sqlContext.getDefaultCluster)}'
        |  AND keyspace_name = '${tableIdent.keyspace}'
        |  AND table_name = '${tableIdent.table}'
      """.stripMargin.replaceAll("\n", " ")

    metaStoreConn.withSessionDo {
      session =>
        val result = session.execute(selectQuery)
        if (result.isExhausted()) {
          None
        } else {
          val row: Row = result.one()
          val options : java.util.Map[String, String] = row.getMap("options", classOf[String], classOf[String])
          val schemaJsonString =
            Option(row.getString("schema_json")).getOrElse(options.get(CassandraDataSourceUserDefinedSchemaNameProperty))
          val schema : Option[StructType] =
            Option(schemaJsonString).map(DataType.fromJson).map(_.asInstanceOf[StructType])

          // convert to scala Map
          import scala.collection.JavaConversions._
          val source = row.getString("source_provider")
          val relation = ResolvedDataSource(sqlContext, schema, source, options.toMap)
          Option(LogicalRelation(relation.relation))
        }
    }
  }

  /** Create a Relation directly from Cassandra table. It may throw NoSuchTableException if it's not found. */
  private def getTableMayThrowException(tableIdent: TableIdent) : LogicalPlan = {
    findTableFromCassandra(tableIdent)
    val sourceRelation = sqlContext.createCassandraSourceRelation(tableIdent, CassandraDataSourceOptions())
    LogicalRelation(sourceRelation)
  }

  /** Check whether table is in Cassandra */
  private def findTableFromCassandra(tableIdent: TableIdent) : Unit = {
    val clusterName = tableIdent.cluster.getOrElse(sqlContext.getDefaultCluster)
    val conn = new CassandraConnector(sqlContext.getCassandraConnConf(clusterName))
    //Throw NoSuchElementException if can't find table in C*
    try {
      Schema.fromCassandra(conn).keyspaceByName(tableIdent.keyspace).tableByName(tableIdent.table)
    } catch {
      case _:NoSuchElementException => throw new NoSuchTableException
    }
  }

  /** Get metastore schema keyspace name */
  private def getMetaStoreKeyspace() : String = {
    sqlContext.conf.getConf(CassandraDataSourceMetaStoreKeyspaceNameProperty,
      DefaultCassandraDataSourceMetaStoreKeyspaceName)
  }

  /** Get metastore schema table name */
  private def getMetaStoreTable() : String = {
    sqlContext.conf.getConf(CassandraDataSourceMetaStoreTableNameProperty,
      DefaultCassandraDataSourceMetaStoreTableName)
  }

  /** Get cluster name where metastore resides */
  private def getMetaStoreCluster() : String = {
    sqlContext.conf.getConf(CassandraDataSourceMetaStoreClusterNameProperty, sqlContext.getDefaultCluster)
  }

  /** Get the cluster names. Those cluster tables are loaded into metastore */
  private def toLoadClusters(): Seq[String] = {
    val clusters =
      sqlContext.conf.getConf(CassandraDataSourceToLoadClustersProperty, sqlContext.getDefaultCluster)
    clusters.split(",").map(_.trim)
  }
}

object DataSourceMetaStore {
  val DefaultCassandraDataSourceMetaStoreKeyspaceName = "data_source_meta_store"
  val DefaultCassandraDataSourceMetaStoreTableName = "data_source_meta_store"

  val CassandraDataSourceMetaStoreClusterNameProperty = "spark.cassandra.datasource.metastore.cluster";
  val CassandraDataSourceMetaStoreKeyspaceNameProperty = "spark.cassandra.datasource.metastore.keyspace";
  val CassandraDataSourceMetaStoreTableNameProperty = "spark.cassandra.datasource.metastore.table";
  //Separated by comma
  val CassandraDataSourceToLoadClustersProperty = "spark.cassandra.datasource.toload.clusters";

  val Properties = Seq(
    CassandraDataSourceMetaStoreClusterNameProperty,
    CassandraDataSourceMetaStoreKeyspaceNameProperty,
    CassandraDataSourceMetaStoreTableNameProperty,
    CassandraDataSourceToLoadClustersProperty
  )

  val SystemKeyspaces = Set("system", "system_traces", DefaultCassandraDataSourceMetaStoreKeyspaceName)
  val TempDatabaseOrTableName = "TO_BE_DELETED"
}