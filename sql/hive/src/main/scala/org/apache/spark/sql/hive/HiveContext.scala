/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.parse.VariableSubstitution
import org.apache.hadoop.hive.serde2.io.{DateWritable, TimestampWritable}
import org.apache.hadoop.util.VersionInfo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{ExecutedCommand, SetCommand}
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.hive.execution.{AnalyzeTable, DescribeHiveTableCommand, HiveNativeCommand}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 *
 * @since 1.0.0
 */
class HiveContext private[hive](
    sc: SparkContext,
    cacheManager: CacheManager,
    listener: SQLListener,
    @transient private[hive] val executionHive: HiveClientImpl,
    @transient private[hive] val metadataHive: HiveClient,
    isRootContext: Boolean,
    @transient private[sql] val hiveCatalog: HiveExternalCatalog)
  extends SQLContext(sc, cacheManager, listener, isRootContext, hiveCatalog) with Logging {
  self =>

  private def this(sc: SparkContext, execHive: HiveClientImpl, metaHive: HiveClient) {
    this(
      sc,
      new CacheManager,
      SQLContext.createListenerAndUI(sc),
      execHive,
      metaHive,
      true,
      new HiveExternalCatalog(metaHive))
  }

  def this(sc: SparkContext) = {
    this(
      sc,
      HiveContext.newClientForExecution(sc.conf, sc.hadoopConfiguration),
      HiveContext.newClientForMetadata(sc.conf, sc.hadoopConfiguration))
  }

  def this(sc: JavaSparkContext) = this(sc.sc)

  import org.apache.spark.sql.hive.HiveContext._

  logDebug("create HiveContext")

  /**
   * Returns a new HiveContext as new session, which will have separated SQLConf, UDF/UDAF,
   * temporary tables and SessionState, but sharing the same CacheManager, IsolatedClientLoader
   * and Hive client (both of execution and metadata) with existing HiveContext.
   */
  override def newSession(): HiveContext = {
    new HiveContext(
      sc = sc,
      cacheManager = cacheManager,
      listener = listener,
      executionHive = executionHive.newSession(),
      metadataHive = metadataHive.newSession(),
      isRootContext = false,
      hiveCatalog = hiveCatalog)
  }

  @transient
  protected[sql] override lazy val sessionState = new HiveSessionState(self)

  /**
   * When true, enables an experimental feature where metastore tables that use the parquet SerDe
   * are automatically converted to use the Spark SQL parquet table scan, instead of the Hive
   * SerDe.
   */
  protected[sql] def convertMetastoreParquet: Boolean = getConf(CONVERT_METASTORE_PARQUET)

  /**
   * When true, also tries to merge possibly different but compatible Parquet schemas in different
   * Parquet data files.
   *
   * This configuration is only effective when "spark.sql.hive.convertMetastoreParquet" is true.
   */
  protected[sql] def convertMetastoreParquetWithSchemaMerging: Boolean =
    getConf(CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING)

  /**
   * When true, enables an experimental feature where metastore tables that use the Orc SerDe
   * are automatically converted to use the Spark SQL ORC table scan, instead of the Hive
   * SerDe.
   */
  protected[sql] def convertMetastoreOrc: Boolean = getConf(CONVERT_METASTORE_ORC)

  /**
   * When true, a table created by a Hive CTAS statement (no USING clause) will be
   * converted to a data source table, using the data source set by spark.sql.sources.default.
   * The table in CTAS statement will be converted when it meets any of the following conditions:
   *   - The CTAS does not specify any of a SerDe (ROW FORMAT SERDE), a File Format (STORED AS), or
   *     a Storage Hanlder (STORED BY), and the value of hive.default.fileformat in hive-site.xml
   *     is either TextFile or SequenceFile.
   *   - The CTAS statement specifies TextFile (STORED AS TEXTFILE) as the file format and no SerDe
   *     is specified (no ROW FORMAT SERDE clause).
   *   - The CTAS statement specifies SequenceFile (STORED AS SEQUENCEFILE) as the file format
   *     and no SerDe is specified (no ROW FORMAT SERDE clause).
   */
  protected[sql] def convertCTAS: Boolean = getConf(CONVERT_CTAS)

  /*
   * hive thrift server use background spark sql thread pool to execute sql queries
   */
  protected[hive] def hiveThriftServerAsync: Boolean = getConf(HIVE_THRIFT_SERVER_ASYNC)

  protected[hive] def hiveThriftServerSingleSession: Boolean =
    sc.conf.get("spark.sql.hive.thriftServer.singleSession", "false").toBoolean

  @transient
  protected[sql] lazy val substitutor = new VariableSubstitution()

  /**
   * Overrides default Hive configurations to avoid breaking changes to Spark SQL users.
   *  - allow SQL11 keywords to be used as identifiers
   */
  private[sql] def defaultOverrides() = {
    setConf(ConfVars.HIVE_SUPPORT_SQL11_RESERVED_KEYWORDS.varname, "false")
  }

  defaultOverrides()

  protected[sql] override def parseSql(sql: String): LogicalPlan = {
    executionHive.withHiveState {
      super.parseSql(substitutor.substitute(hiveconf, sql))
    }
  }

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution(plan)

  /**
   * Invalidate and refresh all the cached the metadata of the given table. For performance reasons,
   * Spark SQL or the external data source library it uses might cache certain metadata about a
   * table, such as the location of blocks. When those change outside of Spark SQL, users should
   * call this function to invalidate the cache.
   *
   * @since 1.3.0
   */
  def refreshTable(tableName: String): Unit = {
    val tableIdent = sessionState.sqlParser.parseTableIdentifier(tableName)
    sessionState.catalog.refreshTable(tableIdent)
  }

  protected[hive] def invalidateTable(tableName: String): Unit = {
    val tableIdent = sessionState.sqlParser.parseTableIdentifier(tableName)
    sessionState.catalog.invalidateTable(tableIdent)
  }

  /**
   * Analyzes the given table in the current database to generate statistics, which will be
   * used in query optimizations.
   *
   * Right now, it only supports Hive tables and it only updates the size of a Hive table
   * in the Hive metastore.
   *
   * @since 1.2.0
   */
  def analyze(tableName: String) {
    AnalyzeTable(tableName).run(self)
  }

  override def setConf(key: String, value: String): Unit = {
    super.setConf(key, value)
    executionHive.runSqlHive(s"SET $key=$value")
    metadataHive.runSqlHive(s"SET $key=$value")
    // If users put any Spark SQL setting in the spark conf (e.g. spark-defaults.conf),
    // this setConf will be called in the constructor of the SQLContext.
    // Also, calling hiveconf will create a default session containing a HiveConf, which
    // will interfer with the creation of executionHive (which is a lazy val). So,
    // we put hiveconf.set at the end of this method.
    hiveconf.set(key, value)
  }

  override private[sql] def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    setConf(entry.key, entry.stringConverter(value))
  }

  /**
   * SQLConf and HiveConf contracts:
   *
   * 1. create a new o.a.h.hive.ql.session.SessionState for each HiveContext
   * 2. when the Hive session is first initialized, params in HiveConf will get picked up by the
   *    SQLConf.  Additionally, any properties set by set() or a SET command inside sql() will be
   *    set in the SQLConf *as well as* in the HiveConf.
   */
  @transient
  protected[hive] lazy val hiveconf: HiveConf = {
    val c = executionHive.conf
    setConf(c.getAllProperties)
    c
  }

  private def functionOrMacroDDLPattern(command: String) = Pattern.compile(
    ".*(create|drop)\\s+(temporary\\s+)?(function|macro).+", Pattern.DOTALL).matcher(command)

  protected[hive] def runSqlHive(sql: String): Seq[String] = {
    val command = sql.trim.toLowerCase
    if (functionOrMacroDDLPattern(command).matches()) {
      executionHive.runSqlHive(sql)
    } else if (command.startsWith("set")) {
      metadataHive.runSqlHive(sql)
      executionHive.runSqlHive(sql)
    } else {
      metadataHive.runSqlHive(sql)
    }
  }

  /**
   * Executes a SQL query without parsing it, but instead passing it directly to Hive.
   * This is currently only used for DDLs and will be removed as soon as Spark can parse
   * all supported Hive DDLs itself.
   */
  protected[sql] override def runNativeSql(sqlText: String): Seq[Row] = {
    runSqlHive(sqlText).map { s => Row(s) }
  }

  /** Extends QueryExecution with hive specific features. */
  protected[sql] class QueryExecution(logicalPlan: LogicalPlan)
    extends org.apache.spark.sql.execution.QueryExecution(this, logicalPlan) {

    /**
     * Returns the result as a hive compatible sequence of strings.  For native commands, the
     * execution is simply passed back to Hive.
     */
    def stringResult(): Seq[String] = executedPlan match {
      case ExecutedCommand(desc: DescribeHiveTableCommand) =>
        // If it is a describe command for a Hive table, we want to have the output format
        // be similar with Hive.
        desc.run(self).map {
          case Row(name: String, dataType: String, comment) =>
            Seq(name, dataType,
              Option(comment.asInstanceOf[String]).getOrElse(""))
              .map(s => String.format(s"%-20s", s))
              .mkString("\t")
        }
      case command: ExecutedCommand =>
        command.executeCollect().map(_.getString(0))

      case other =>
        val result: Seq[Seq[Any]] = other.executeCollectPublic().map(_.toSeq).toSeq
        // We need the types so we can output struct field names
        val types = analyzed.output.map(_.dataType)
        // Reformat to match hive tab delimited output.
        result.map(_.zip(types).map(HiveContext.toHiveString)).map(_.mkString("\t")).toSeq
    }

    override def simpleString: String =
      logical match {
        case _: HiveNativeCommand => "<Native command: executed by Hive>"
        case _: SetCommand => "<SET command: executed by Hive, and noted by SQLContext>"
        case _ => super.simpleString
      }
  }

  protected[sql] override def addJar(path: String): Unit = {
    // Add jar to Hive and classloader
    executionHive.addJar(path)
    metadataHive.addJar(path)
    Thread.currentThread().setContextClassLoader(executionHive.clientLoader.classLoader)
    super.addJar(path)
  }
}


private[hive] object HiveContext extends Logging {
  /** The version of hive used internally by Spark SQL. */
  val hiveExecutionVersion: String = "1.2.1"

  val HIVE_METASTORE_VERSION = SQLConfigBuilder("spark.sql.hive.metastore.version")
    .doc("Version of the Hive metastore. Available options are " +
        s"<code>0.12.0</code> through <code>$hiveExecutionVersion</code>.")
    .stringConf
    .createWithDefault(hiveExecutionVersion)

  val HIVE_EXECUTION_VERSION = SQLConfigBuilder("spark.sql.hive.version")
    .doc("Version of Hive used internally by Spark SQL.")
    .stringConf
    .createWithDefault(hiveExecutionVersion)

  val HIVE_METASTORE_JARS = SQLConfigBuilder("spark.sql.hive.metastore.jars")
    .doc(s"""
      | Location of the jars that should be used to instantiate the HiveMetastoreClient.
      | This property can be one of three options: "
      | 1. "builtin"
      |   Use Hive ${hiveExecutionVersion}, which is bundled with the Spark assembly when
      |   <code>-Phive</code> is enabled. When this option is chosen,
      |   <code>spark.sql.hive.metastore.version</code> must be either
      |   <code>${hiveExecutionVersion}</code> or not defined.
      | 2. "maven"
      |   Use Hive jars of specified version downloaded from Maven repositories.
      | 3. A classpath in the standard format for both Hive and Hadoop.
      """.stripMargin)
    .stringConf
    .createWithDefault("builtin")

  val CONVERT_METASTORE_PARQUET = SQLConfigBuilder("spark.sql.hive.convertMetastoreParquet")
    .doc("When set to false, Spark SQL will use the Hive SerDe for parquet tables instead of " +
      "the built in support.")
    .booleanConf
    .createWithDefault(true)

  val CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING =
    SQLConfigBuilder("spark.sql.hive.convertMetastoreParquet.mergeSchema")
      .doc("When true, also tries to merge possibly different but compatible Parquet schemas in " +
        "different Parquet data files. This configuration is only effective " +
        "when \"spark.sql.hive.convertMetastoreParquet\" is true.")
      .booleanConf
      .createWithDefault(false)

  val CONVERT_CTAS = SQLConfigBuilder("spark.sql.hive.convertCTAS")
    .doc("When true, a table created by a Hive CTAS statement (no USING clause) will be " +
      "converted to a data source table, using the data source set by spark.sql.sources.default.")
    .booleanConf
    .createWithDefault(false)

  val CONVERT_METASTORE_ORC = SQLConfigBuilder("spark.sql.hive.convertMetastoreOrc")
    .doc("When set to false, Spark SQL will use the Hive SerDe for ORC tables instead of " +
      "the built in support.")
    .booleanConf
    .createWithDefault(true)

  val HIVE_METASTORE_SHARED_PREFIXES = SQLConfigBuilder("spark.sql.hive.metastore.sharedPrefixes")
    .doc("A comma separated list of class prefixes that should be loaded using the classloader " +
      "that is shared between Spark SQL and a specific version of Hive. An example of classes " +
      "that should be shared is JDBC drivers that are needed to talk to the metastore. Other " +
      "classes that need to be shared are those that interact with classes that are already " +
      "shared. For example, custom appenders that are used by log4j.")
    .stringConf
    .toSequence
    .createWithDefault(jdbcPrefixes)

  private def jdbcPrefixes = Seq(
    "com.mysql.jdbc", "org.postgresql", "com.microsoft.sqlserver", "oracle.jdbc")

  val HIVE_METASTORE_BARRIER_PREFIXES = SQLConfigBuilder("spark.sql.hive.metastore.barrierPrefixes")
    .doc("A comma separated list of class prefixes that should explicitly be reloaded for each " +
      "version of Hive that Spark SQL is communicating with. For example, Hive UDFs that are " +
      "declared in a prefix that typically would be shared (i.e. <code>org.apache.spark.*</code>).")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val HIVE_THRIFT_SERVER_ASYNC = SQLConfigBuilder("spark.sql.hive.thriftServer.async")
    .doc("When set to true, Hive Thrift server executes SQL queries in an asynchronous way.")
    .booleanConf
    .createWithDefault(true)

  /**
   * The version of the hive client that will be used to communicate with the metastore.  Note that
   * this does not necessarily need to be the same version of Hive that is used internally by
   * Spark SQL for execution.
   */
  private def hiveMetastoreVersion(conf: SQLConf): String = {
    conf.getConf(HIVE_METASTORE_VERSION)
  }

  /**
   * The location of the jars that should be used to instantiate the HiveMetastoreClient.  This
   * property can be one of three options:
   *  - a classpath in the standard format for both hive and hadoop.
   *  - builtin - attempt to discover the jars that were used to load Spark SQL and use those. This
   *              option is only valid when using the execution version of Hive.
   *  - maven - download the correct version of hive on demand from maven.
   */
  private def hiveMetastoreJars(conf: SQLConf): String = {
    conf.getConf(HIVE_METASTORE_JARS)
  }

  /**
   * A comma separated list of class prefixes that should be loaded using the classloader that
   * is shared between Spark SQL and a specific version of Hive. An example of classes that should
   * be shared is JDBC drivers that are needed to talk to the metastore. Other classes that need
   * to be shared are those that interact with classes that are already shared.  For example,
   * custom appenders that are used by log4j.
   */
  private def hiveMetastoreSharedPrefixes(conf: SQLConf): Seq[String] = {
    conf.getConf(HIVE_METASTORE_SHARED_PREFIXES).filterNot(_ == "")
  }

  /**
   * A comma separated list of class prefixes that should explicitly be reloaded for each version
   * of Hive that Spark SQL is communicating with.  For example, Hive UDFs that are declared in a
   * prefix that typically would be shared (i.e. org.apache.spark.*)
   */
  private def hiveMetastoreBarrierPrefixes(conf: SQLConf): Seq[String] = {
    conf.getConf(HIVE_METASTORE_BARRIER_PREFIXES).filterNot(_ == "")
  }

  /**
   * Configurations needed to create a [[HiveClient]].
   */
  private[hive] def hiveClientConfigurations(hiveconf: HiveConf): Map[String, String] = {
    // Hive 0.14.0 introduces timeout operations in HiveConf, and changes default values of a bunch
    // of time `ConfVar`s by adding time suffixes (`s`, `ms`, and `d` etc.).  This breaks backwards-
    // compatibility when users are trying to connecting to a Hive metastore of lower version,
    // because these options are expected to be integral values in lower versions of Hive.
    //
    // Here we enumerate all time `ConfVar`s and convert their values to numeric strings according
    // to their output time units.
    Seq(
      ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY -> TimeUnit.SECONDS,
      ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.METASTORE_CLIENT_SOCKET_LIFETIME -> TimeUnit.SECONDS,
      ConfVars.HMSHANDLERINTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.METASTORE_EVENT_DB_LISTENER_TTL -> TimeUnit.SECONDS,
      ConfVars.METASTORE_EVENT_CLEAN_FREQ -> TimeUnit.SECONDS,
      ConfVars.METASTORE_EVENT_EXPIRY_DURATION -> TimeUnit.SECONDS,
      ConfVars.METASTORE_AGGREGATE_STATS_CACHE_TTL -> TimeUnit.SECONDS,
      ConfVars.METASTORE_AGGREGATE_STATS_CACHE_MAX_WRITER_WAIT -> TimeUnit.MILLISECONDS,
      ConfVars.METASTORE_AGGREGATE_STATS_CACHE_MAX_READER_WAIT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVES_AUTO_PROGRESS_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_LOG_INCREMENTAL_PLAN_PROGRESS_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_STATS_JDBC_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_STATS_RETRIES_WAIT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES -> TimeUnit.SECONDS,
      ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_TXN_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_COMPACTOR_CHECK_INTERVAL -> TimeUnit.SECONDS,
      ConfVars.HIVE_COMPACTOR_CLEANER_RUN_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_MAX_IDLE_TIME -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_WORKER_KEEPALIVE_TIME -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_MAX_AGE -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_WORKER_KEEPALIVE_TIME -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_SESSION_CHECK_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_IDLE_SESSION_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_IDLE_OPERATION_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.SERVER_READ_SOCKET_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_LOCALIZE_RESOURCE_WAIT_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.SPARK_CLIENT_FUTURE_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.SPARK_JOB_MONITOR_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.SPARK_RPC_CLIENT_CONNECT_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.SPARK_RPC_CLIENT_HANDSHAKE_TIMEOUT -> TimeUnit.MILLISECONDS
    ).map { case (confVar, unit) =>
      confVar.varname -> hiveconf.getTimeVar(confVar, unit).toString
    }.toMap
  }

  /**
   * Create a [[HiveClient]] used for execution.
   *
   * Currently this must always be Hive 13 as this is the version of Hive that is packaged
   * with Spark SQL. This copy of the client is used for execution related tasks like
   * registering temporary functions or ensuring that the ThreadLocal SessionState is
   * correctly populated.  This copy of Hive is *not* used for storing persistent metadata,
   * and only point to a dummy metastore in a temporary directory.
   */
  protected[hive] def newClientForExecution(
      conf: SparkConf,
      hadoopConf: Configuration): HiveClientImpl = {
    logInfo(s"Initializing execution hive, version $hiveExecutionVersion")
    val loader = new IsolatedClientLoader(
      version = IsolatedClientLoader.hiveVersion(hiveExecutionVersion),
      sparkConf = conf,
      execJars = Seq(),
      hadoopConf = hadoopConf,
      config = newTemporaryConfiguration(useInMemoryDerby = true),
      isolationOn = false,
      baseClassLoader = Utils.getContextOrSparkClassLoader)
    loader.createClient().asInstanceOf[HiveClientImpl]
  }

  /**
   * Create a [[HiveClient]] used to retrieve metadata from the Hive MetaStore.
   *
   * The version of the Hive client that is used here must match the metastore that is configured
   * in the hive-site.xml file.
   */
  private def newClientForMetadata(conf: SparkConf, hadoopConf: Configuration): HiveClient = {
    val hiveConf = new HiveConf(hadoopConf, classOf[HiveConf])
    val configurations = hiveClientConfigurations(hiveConf)
    newClientForMetadata(conf, hiveConf, hadoopConf, configurations)
  }

  protected[hive] def newClientForMetadata(
      conf: SparkConf,
      hiveConf: HiveConf,
      hadoopConf: Configuration,
      configurations: Map[String, String]): HiveClient = {
    val sqlConf = new SQLConf
    sqlConf.setConf(SQLContext.getSQLProperties(conf))
    val hiveMetastoreVersion = HiveContext.hiveMetastoreVersion(sqlConf)
    val hiveMetastoreJars = HiveContext.hiveMetastoreJars(sqlConf)
    val hiveMetastoreSharedPrefixes = HiveContext.hiveMetastoreSharedPrefixes(sqlConf)
    val hiveMetastoreBarrierPrefixes = HiveContext.hiveMetastoreBarrierPrefixes(sqlConf)
    val metaVersion = IsolatedClientLoader.hiveVersion(hiveMetastoreVersion)

    val defaultWarehouseLocation = hiveConf.get("hive.metastore.warehouse.dir")
    logInfo("default warehouse location is " + defaultWarehouseLocation)

    // `configure` goes second to override other settings.
    val allConfig = hiveConf.asScala.map(e => e.getKey -> e.getValue).toMap ++ configurations

    val isolatedLoader = if (hiveMetastoreJars == "builtin") {
      if (hiveExecutionVersion != hiveMetastoreVersion) {
        throw new IllegalArgumentException(
          "Builtin jars can only be used when hive execution version == hive metastore version. " +
            s"Execution: $hiveExecutionVersion != Metastore: $hiveMetastoreVersion. " +
            "Specify a vaild path to the correct hive jars using $HIVE_METASTORE_JARS " +
            s"or change ${HIVE_METASTORE_VERSION.key} to $hiveExecutionVersion.")
      }

      // We recursively find all jars in the class loader chain,
      // starting from the given classLoader.
      def allJars(classLoader: ClassLoader): Array[URL] = classLoader match {
        case null => Array.empty[URL]
        case urlClassLoader: URLClassLoader =>
          urlClassLoader.getURLs ++ allJars(urlClassLoader.getParent)
        case other => allJars(other.getParent)
      }

      val classLoader = Utils.getContextOrSparkClassLoader
      val jars = allJars(classLoader)
      if (jars.length == 0) {
        throw new IllegalArgumentException(
          "Unable to locate hive jars to connect to metastore. " +
            "Please set spark.sql.hive.metastore.jars.")
      }

      logInfo(
        s"Initializing HiveMetastoreConnection version $hiveMetastoreVersion using Spark classes.")
      new IsolatedClientLoader(
        version = metaVersion,
        sparkConf = conf,
        hadoopConf = hadoopConf,
        execJars = jars.toSeq,
        config = allConfig,
        isolationOn = true,
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    } else if (hiveMetastoreJars == "maven") {
      // TODO: Support for loading the jars from an already downloaded location.
      logInfo(
        s"Initializing HiveMetastoreConnection version $hiveMetastoreVersion using maven.")
      IsolatedClientLoader.forVersion(
        hiveMetastoreVersion = hiveMetastoreVersion,
        hadoopVersion = VersionInfo.getVersion,
        sparkConf = conf,
        hadoopConf = hadoopConf,
        config = allConfig,
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    } else {
      // Convert to files and expand any directories.
      val jars =
        hiveMetastoreJars
          .split(File.pathSeparator)
          .flatMap {
          case path if new File(path).getName == "*" =>
            val files = new File(path).getParentFile.listFiles()
            if (files == null) {
              logWarning(s"Hive jar path '$path' does not exist.")
              Nil
            } else {
              files.filter(_.getName.toLowerCase.endsWith(".jar"))
            }
          case path =>
            new File(path) :: Nil
        }
          .map(_.toURI.toURL)

      logInfo(
        s"Initializing HiveMetastoreConnection version $hiveMetastoreVersion " +
          s"using ${jars.mkString(":")}")
      new IsolatedClientLoader(
        version = metaVersion,
        sparkConf = conf,
        hadoopConf = hadoopConf,
        execJars = jars.toSeq,
        config = allConfig,
        isolationOn = true,
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    }
    isolatedLoader.createClient()
  }

  /** Constructs a configuration for hive, where the metastore is located in a temp directory. */
  def newTemporaryConfiguration(useInMemoryDerby: Boolean): Map[String, String] = {
    val withInMemoryMode = if (useInMemoryDerby) "memory:" else ""

    val tempDir = Utils.createTempDir()
    val localMetastore = new File(tempDir, "metastore")
    val propMap: HashMap[String, String] = HashMap()
    // We have to mask all properties in hive-site.xml that relates to metastore data source
    // as we used a local metastore here.
    HiveConf.ConfVars.values().foreach { confvar =>
      if (confvar.varname.contains("datanucleus") || confvar.varname.contains("jdo")
        || confvar.varname.contains("hive.metastore.rawstore.impl")) {
        propMap.put(confvar.varname, confvar.getDefaultExpr())
      }
    }
    propMap.put(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, localMetastore.toURI.toString)
    propMap.put(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
      s"jdbc:derby:${withInMemoryMode};databaseName=${localMetastore.getAbsolutePath};create=true")
    propMap.put("datanucleus.rdbms.datastoreAdapterClassName",
      "org.datanucleus.store.rdbms.adapter.DerbyAdapter")

    // SPARK-11783: When "hive.metastore.uris" is set, the metastore connection mode will be
    // remote (https://cwiki.apache.org/confluence/display/Hive/AdminManual+MetastoreAdmin
    // mentions that "If hive.metastore.uris is empty local mode is assumed, remote otherwise").
    // Remote means that the metastore server is running in its own process.
    // When the mode is remote, configurations like "javax.jdo.option.ConnectionURL" will not be
    // used (because they are used by remote metastore server that talks to the database).
    // Because execution Hive should always connects to a embedded derby metastore.
    // We have to remove the value of hive.metastore.uris. So, the execution Hive client connects
    // to the actual embedded derby metastore instead of the remote metastore.
    // You can search HiveConf.ConfVars.METASTOREURIS in the code of HiveConf (in Hive's repo).
    // Then, you will find that the local metastore mode is only set to true when
    // hive.metastore.uris is not set.
    propMap.put(ConfVars.METASTOREURIS.varname, "")

    propMap.toMap
  }

  protected val primitiveTypes =
    Seq(StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, ByteType,
      ShortType, DateType, TimestampType, BinaryType)

  protected[sql] def toHiveString(a: (Any, DataType)): String = a match {
    case (struct: Row, StructType(fields)) =>
      struct.toSeq.zip(fields).map {
        case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
      }.mkString("{", ",", "}")
    case (seq: Seq[_], ArrayType(typ, _)) =>
      seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
    case (map: Map[_, _], MapType(kType, vType, _)) =>
      map.map {
        case (key, value) =>
          toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
      }.toSeq.sorted.mkString("{", ",", "}")
    case (null, _) => "NULL"
    case (d: Int, DateType) => new DateWritable(d).toString
    case (t: Timestamp, TimestampType) => new TimestampWritable(t).toString
    case (bin: Array[Byte], BinaryType) => new String(bin, StandardCharsets.UTF_8)
    case (decimal: java.math.BigDecimal, DecimalType()) =>
      // Hive strips trailing zeros so use its toString
      HiveDecimal.create(decimal).toString
    case (other, tpe) if primitiveTypes contains tpe => other.toString
  }

  /** Hive outputs fields of structs slightly differently than top level attributes. */
  protected def toHiveStructString(a: (Any, DataType)): String = a match {
    case (struct: Row, StructType(fields)) =>
      struct.toSeq.zip(fields).map {
        case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
      }.mkString("{", ",", "}")
    case (seq: Seq[_], ArrayType(typ, _)) =>
      seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
    case (map: Map[_, _], MapType(kType, vType, _)) =>
      map.map {
        case (key, value) =>
          toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
      }.toSeq.sorted.mkString("{", ",", "}")
    case (null, _) => "null"
    case (s: String, StringType) => "\"" + s + "\""
    case (decimal, DecimalType()) => decimal.toString
    case (other, tpe) if primitiveTypes contains tpe => other.toString
  }
}