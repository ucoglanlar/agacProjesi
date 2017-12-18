package com.sensity.rollup

/**
  * NSN-3094
  * Created by iozsaracoglu on 12/13/16.
  *
  * This spark application computes energy savings (in kWh) for all nodes as we receive actual power sensor samples.
  * These computed energy savings values are stored in a Cassandra table and they will be available for reporting.
  * Reports will not have to do the computation heavy lifting as we will have the calculated values prepared ahead of time.
  * We are computing energy savings values for a specified time resolution, which can be changed anytime.
  * The current default resolution will be 15 minutes. Note that, changing the computation resolution may require us to re-run the
  * process on existing data. Otherwise the new resolution will be in effect for the new incoming data.
  * Aggregations to 1-hr and 1-day are also being computed at the node level.
  * Report may still need to be performing final roll-ups depending on the reporting window start-end times, and for higher level groupings
  * like site or customer level.
  *
  */

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.joda.time._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration


object Starter {

  case class DeviceSensorSamples(nodeid: String, sensor: String, date: String, time: Long, value: Double )
  case class DeviceEnergySettings(siteid: String, nodeid: String, legacy_power: Double, max_led_power: Double)
  case class AggregationControl(typ: String, key: String, value: String)
  case class AggregationBucket(siteid: String, nodeid: String, aggregation_type: String, starttime: Long,
                               actual_energy_consumption: Double, enddt: String, endtime: Long,
                               led_energy_consumption: Double, legacy_energy_consumption: Double,
                               savings_legacy_vs_actual: Double, savings_legacy_vs_led: Double,
                               startday: String, startdt: String, starthr: String, ts: Long
                              )

  val StructDeviceEnergySettings = StructType(
    StructField("siteid", StringType, false) ::
    StructField("nodeid", StringType, false) ::
    StructField("legacy_power", DoubleType, false) ::
    StructField("max_led_power", DoubleType, false) ::
  Nil)

  val StructEnergySavings = StructType(
    StructField("siteid", StringType, false) ::
    StructField("nodeid", StringType, false) ::
    StructField("aggregation_type", StringType, false) ::
    StructField("starttime", LongType, false) ::
    StructField("actual_energy_consumption", DoubleType, false) ::
    StructField("enddt", StringType, false) ::
    StructField("endtime", LongType, false) ::
    StructField("led_energy_consumption", DoubleType, false) ::
    StructField("legacy_energy_consumption", DoubleType, false) ::
    StructField("savings_legacy_vs_actual", DoubleType, false) ::
    StructField("savings_legacy_vs_led", DoubleType, false) ::
    StructField("startday", StringType, false) ::
    StructField("startdt", StringType, false) ::
    StructField("starthr", StringType, false) ::
    StructField("ts", LongType, false) ::
   Nil)

  val SparkAppName = "energy-rollup"

  // 15 min bucket size (in microseconds) as the lowest level time window for rollups - this is the aggregation resolution
  val bucketSize = 1000000*60*15;

  /*
  Run parameters with default values are listed below. They should be passed to this application during the spark-submit step initiated by the
  scheduled job.
  */
  var inp_cassIP = "0.0.0.0" // Use QA Cassandra instance as default. Must be passed for production runs.

  var inp_neoIP = "0.0.0.0" // Use QA Neo4j instance as default. Must be passed for production runs.

  var inp_runMode = "TEST_LOCAL"  // Run in local[*] mode as default. Must be passed for production runs.
  // Accepted values: TEST_LOCAL, TEST_YARN, PROD_YARN, PROD_LOCAL.

  var inp_version = "D"

  var inp_startFromDT = "NONE" // In UTC yyyy-MM-dd HH:mm format. If passed, will start the rollup at the most recent top of the hour of this value. If
  // not passed, HWM will be used. Parameter should be used for testing only.
  var inp_processUntilDT = "NONE" // In UTC yyyy-MM-dd HH:mm format. If passed, will end the rollup at the most recent top of the hour of this value. If
  // not passed, "now" will be used. Parameter should be used for testing only.

  var inp_eventRecordLimit = "NONE" // If passed, will limit the number of event records per site to be processed. Should be used for testing only.

  var inp_site = "ALL" // If passed, will process records for a single site. Should be used for testing  only.

  var inp_sparkCleanerTTL = "3600"
  var inp_cassKeyspace = "farallones"
  var inp_cassPort = "9042"
  var inp_neoPort = "7474"

  var HWM_LIMIT_MINS = "na"

  /*
  TODO:
  This process should run in a job scheduler (cronjob, etc.) in regular intervals, like every hour, or more frequently depending on rollup speed
  and processing window size. Processing window is a moving window of time.
  */

  val rootLogger = Logger.getRootLogger
  val sparkLogging = Logger.getLogger("org.apache.spark")
  val cassandraLogging = Logger.getLogger("com.datastax")

  // This is used to determine device parameter for power reading
  var POWER_PARAMETER = "mP" // default

  // This is used to treat missing power samples
  var POWER_SAMPLING_INTERVAL_SEC = 6000 // default is 10 minutes

  // Power may never reach zero even when the lights are completely off
  var THRESHOLD_ZERO_POWER = 0.1d // default

  // Rollup variables for time window
  var dtLast = DateTime.now(DateTimeZone.UTC)
  val timeEnd = DateTime.now(DateTimeZone.UTC)
  var rollupUntil = timeEnd.withMinuteOfHour(0)

  // Used for testing
  var eventLimit = 1000000 // insanity limit for samples per node to process


  def main(args: Array[String]) {

    // Process command line arguments for the spark-submit
    receiveParameters(args)

    InitCassandra.apply()

    if (inp_startFromDT == "NONE") {
      dtLast = HighWaterMark.get() // Get the most recent rollup run time from aggregation_control table
      if (HWM_LIMIT_MINS != "na" && dtLast.plusMinutes(HWM_LIMIT_MINS.toInt).isBeforeNow) {
        dtLast = timeEnd.minusMinutes(HWM_LIMIT_MINS.toInt)
      }
    }
    else
      dtLast = getDateFrom(inp_startFromDT) // Rollup start time passed as input

    // Always start from the 00:00 hour prev day so that we will have complete day for 1day aggregations
    dtLast = dtLast.withMinuteOfHour(0).withHourOfDay(0).minusDays(1)
    //dtLast = dtLast.withMinuteOfHour(0).withHourOfDay(0)

    if (inp_processUntilDT == "NONE")
      rollupUntil = timeEnd // Rollup until top of the hour - until now
    else
      rollupUntil = getDateFrom(inp_processUntilDT) // Rollup end time passed as input

    rootLogger.info("Cassandra Keyspace : " + inp_cassKeyspace)
    rootLogger.info("Cassandra IP       : " + inp_cassIP)
    rootLogger.info("Neo4j IP           : " + inp_neoIP)
    rootLogger.info("Run Mode           : " + inp_runMode)
    rootLogger.info("Version            : " + inp_version)
    rootLogger.info("Rollup Start       : " + inp_startFromDT)
    rootLogger.info("Rollup End         : " + inp_processUntilDT)
    rootLogger.info("Site Event Limit   : " + inp_eventRecordLimit)
    rootLogger.info("Site               : " + inp_site)
    rootLogger.info("Spark Cleaner TTL  : " + inp_sparkCleanerTTL)
    rootLogger.info("Cassandra port     : " + inp_cassPort)
    rootLogger.info("Neo4j port         : " + inp_neoPort)

    rootLogger.info("ExpSamplingInterval: " + POWER_SAMPLING_INTERVAL_SEC.toString)
    rootLogger.info("ZeroPowerThreshold : " + THRESHOLD_ZERO_POWER)
    rootLogger.info("Energy Sensor      : " + POWER_PARAMETER)

    // Processing window: dtLast - rollupUntil.
    rootLogger.info("Rollup started for data after " + dtLast.toString("yyyy-MM-dd HH:mm") +
      " until " + rollupUntil.toString("yyyy-MM-dd HH:mm") )


    // Populate site metadata from Neo4j database
    populateSiteInfo()

    // Prepare an empty dataframe for accumulating node level aggregations
    import JobSessions.spark.implicits._
    JobSessions.spark.emptyDataset[Starter.AggregationBucket].toDF().createOrReplaceTempView("aggnode")

    // Node level aggregations for the time bucket size (resolution)
    ProcessNodes.run(dtLast, rollupUntil, inp_cassKeyspace, Starter.POWER_SAMPLING_INTERVAL_SEC, Starter.THRESHOLD_ZERO_POWER)

    HighWaterMark.write()

    JobSessions.sc.stop()

    rootLogger.info("Energy Rollup Completed in " + ((DateTime.now(DateTimeZone.UTC).getMillis - timeEnd.getMillis)/1000).toInt.toString + " seconds.")
  }

  def populateSiteInfo() = {

    import org.anormcypher._
    import play.api.libs.ws._
    import scala.concurrent.ExecutionContext.Implicits.global

    implicit val wsclient = ning.NingWSClient()
    implicit val connection: Neo4jConnection = Neo4jREST(Starter.inp_neoIP, Starter.inp_neoPort.toInt, "neo4j", "neo4j1")

    try {

      val allSites = Cypher(" MATCH (s:Site)-[:HAS]-> (f:Fixture) -[:HAS]-> (n:Node) " +
        " WHERE  toFloat(replace(replace(f.LegacyPowerDraw,\"W\",\"\"),\"w\",\"\")) > toFloat(replace(replace(f.PowerDraw,\"W\",\"\"),\"w\",\"\")) " +
        " RETURN s.siteid as siteid, n.nodeid as nodeid, toString(f.LegacyPowerDraw) as legacy_power, toString(f.PowerDraw) as max_led_power "
      )

      val listSites = allSites.apply().map(row => {
        var mp = row[Option[String]]("max_led_power").map(_.toString).getOrElse("").replace("W", "").replace("w", "").replace(" ","")
        if (mp.length == 0) mp = "0.0"

        var lpd = row[Option[String]]("legacy_power").map(_.toString).getOrElse("").replace("W", "").replace("w", "").replace(" ","")
        if (lpd.length == 0) lpd = "0.0"

        Row(row[String]("siteid"), row[String]("nodeid"), lpd.toDouble, mp.toDouble)
      }).toList

      val rdd = JobSessions.sc.makeRDD(listSites)
      val dfSites = JobSessions.spark.createDataFrame(rdd, StructDeviceEnergySettings)

      dfSites.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "device_energy_settings", "keyspace" -> Starter.inp_cassKeyspace))
        .mode(SaveMode.Overwrite)
        .save()
    } catch {
      case ex: Exception => {
        println("Neo4j data pull exception: " + ex.getLocalizedMessage)
      }
    } finally {
      wsclient.close()
    }
  }

  def receiveParameters(args: Array[String]) = {
    // Process Args
    if (args.length>0) inp_cassKeyspace = args(0)
    if (args.length>1) inp_cassIP = args(1)
    if (args.length>2) inp_neoIP = args(2)

    if (args.length>3) inp_runMode = args(3)

    if (args.length>4) inp_version = args(4)

    if (args.length>5) inp_startFromDT = args(5)
    if (args.length>6) inp_processUntilDT = args(6)

    if (args.length>7) inp_eventRecordLimit = args(7)
    if (args.length>8) inp_site = args(8)

    if (args.length>9) inp_sparkCleanerTTL = args(9)
    if (args.length>10) inp_cassPort = args(10)
    if (args.length>11) inp_neoPort = args(11)

    System.setProperty("runMode",inp_runMode)

    if (inp_runMode == "TEST_LOCAL" || inp_runMode == "TEST_YARN" ) {
      rootLogger.setLevel(Level.INFO)
      sparkLogging.setLevel(Level.ERROR)
      cassandraLogging.setLevel(Level.ERROR)
    }

    if (inp_eventRecordLimit != "NONE") eventLimit = inp_eventRecordLimit.toInt

    var pVal = "na"
    pVal = Parameters.get("power_parameter")
    if (pVal != "na") POWER_PARAMETER = pVal
    pVal = Parameters.get("power_sampling_interval_sec")
    if (pVal != "na") POWER_SAMPLING_INTERVAL_SEC = pVal.toInt
    pVal = Parameters.get("threshold_zero_power")
    if (pVal != "na") THRESHOLD_ZERO_POWER = pVal.toDouble

    pVal = "na"
    pVal = Parameters.get("hwmlimitmins")
    if (pVal != "na") HWM_LIMIT_MINS = pVal
  }

  // Conversion from yyyy-MM-dd HH:mm to DateTime.
  def getDateFrom(inp: String): DateTime = {
    if (inp == 'na) return new DateTime(1970,1,1,0,0,DateTimeZone.UTC) // Epoch start
    else
      return  new DateTime(
        inp.substring(0,4).toInt,
        inp.substring(5,7).toInt,
        inp.substring(8,10).toInt,
        inp.substring(11,13).toInt,
        inp.substring(14).toInt,
        DateTimeZone.UTC
      )
  }

  // Conversion from DateTime to yyyy-MM-dd HH:mm as string
  def getStringFromDate(inp: DateTime): String = {
    return inp.toString("yyyy-MM-dd HH:mm")
  }

  // Conversion from timestamp of microseconds to yyyy-MM-dd HH:mm as string
  def getStringFromMicrosecs(inp: Long): String = {
    return getStringFromDate(new DateTime(inp/1000, DateTimeZone.UTC))
  }

  def pivotGroup(dfIn:DataFrame, groupSize:Int, dataType: String): DataFrame = {

    val StructGroup = StructType(
      StructField("grp", StringType, true) ::
        Nil)

    var listOut = ListBuffer[Row]()
    var gcnt = 0
    var grp = ""
    dfIn.collect().foreach(r => {
      var obj = r.getString(0)
      gcnt += 1

      if (dataType == "String") {
        if (gcnt > 1) grp += ","
        grp += "'"+obj+"'"
      }
      else {
        if (gcnt > 1) grp += ","
        grp += obj
      }

      if (gcnt == groupSize) {
        listOut += Row(grp)
        gcnt = 0
        grp = ""
      }
    })
    if (gcnt > 0) listOut += Row(grp)

    val list = listOut.toList
    val rdd = JobSessions.sc.makeRDD(list)
    val dfOut = JobSessions.spark.createDataFrame(rdd, StructGroup)
    listOut.clear()

    return dfOut
  }

}


object InitCassandra {

  def apply() = {
    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

    var stmt =
      " insert into " +  Starter.inp_cassKeyspace + ".aggregation_control(type, key, val) " +
      " values ('energy','last_agg_min','" + DateTime.now(DateTimeZone.UTC).minusMinutes(60).toString("yyyy-MM-dd HH:mm") + "') if not exists "
    cassSession.execute(stmt)

    stmt =
      " insert into " +  Starter.inp_cassKeyspace + ".aggregation_control(type, key, val) " +
      " values ('energy','power_parameter','mP') if not exists "
    cassSession.execute(stmt)

    stmt =
      " insert into " +  Starter.inp_cassKeyspace + ".aggregation_control(type, key, val) " +
      " values ('energy','threshold_zero_power','0.1') if not exists "
    cassSession.execute(stmt)

    stmt =
      " insert into " +  Starter.inp_cassKeyspace + ".aggregation_control(type, key, val) " +
      " values ('energy','power_sampling_interval_sec','6000') if not exists "
    cassSession.execute(stmt)

    cassSession.close()
  }

}


object Parameters {
  /*
  Energy Savings algorithm parameters
  */

  def get(pName:String) :String = {

    var pVal = "na"

    val cass_table = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "aggregation_control")
    val c1 = cass_table.where(
      "       type = 'energy' " +
      "   and key = '" + pName + "' "
    )
    val c2 = c1.limit(1L)

    val rdd = c2.map(r => new Starter.AggregationControl(
      r.getString("type"),
      r.getString("key"),
      r.getString("val")
    ))

    val mDF = JobSessions.spark.createDataFrame(rdd)

    mDF.collect().foreach(r => {
      if (r.getString(1) == pName)  pVal = r.getString(2)
    })

    return pVal
  }
}

object HighWaterMark {
  /*
  High water mark for the rollup runs are persisted in a Cassandra table.
  */

  def get(): DateTime = {

    val cass_table = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "aggregation_control")
    val c1 = cass_table.where(
      "       type = 'energy' " +
        "and  key = 'last_agg_min' "
    )
    val c2 = c1.limit(1L)

    val rdd = c2.map(r => new Starter.AggregationControl(
      r.getString("type"),
      r.getString("key"),
      r.getString("val")
    ))

    val mDF = JobSessions.spark.createDataFrame(rdd)

    var last_agg_min = "na"
    mDF.collect().foreach(r=> {
      last_agg_min = r.getString(2)
    })

    return Starter.getDateFrom(last_agg_min)
  }

  def write() = {

    if (Starter.inp_eventRecordLimit == "NONE" && Starter.inp_startFromDT == "NONE"
    && Starter.inp_processUntilDT == "NONE" && Starter.inp_site == "ALL") {
      /*
      Writes high-water-mark for the next instance of roll-up.
      */
      Starter.rootLogger.info("Adjusting HWM...")
      val collection = JobSessions.sc.parallelize(Seq((
        "energy", "last_agg_min", Starter.timeEnd.toString("yyyy-MM-dd HH:mm"))
      ))
      collection.saveToCassandra(Starter.inp_cassKeyspace, "aggregation_control",
        SomeColumns(
          "type", "key", "val")
      )
    }
    else {
      Starter.rootLogger.info("HWM not changed.")
    }
  }

}


object JobSessions {
  Starter.rootLogger.info("Connecting to Sessions...")

  val CassandraSeed = Starter.inp_cassIP
  val KeySpace = Starter.inp_cassKeyspace
  val CassandraPort = Starter.inp_cassPort

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraSeed)
    .set("spark.cassandra.connection.port", CassandraPort)
    .set("spark.cassandra.input.consistency.level", "ONE")
    .set("spark.cleaner.ttl", Starter.inp_sparkCleanerTTL)
    .setAppName(Starter.SparkAppName)

  if (Starter.inp_runMode == "TEST_LOCAL" || Starter.inp_runMode == "PROD_LOCAL" ) conf.setMaster("local[*]")

  val spark = SparkSession
    .builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  val sc = spark.sparkContext

  Starter.rootLogger.info("Connected to Sessions.")
}

object GetNodesData {

  var DFjoinedData = JobSessions.spark.sql("create temporary view x as select 'a' as fl ")

  /*
  Generate data set for legacy and led power setup of the nodes.
  */
  def apply(from:String, until:String): DataFrame = {

    Starter.rootLogger.info("from  : " + from )
    Starter.rootLogger.info("until : " + until )

    val cass_table = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "device_energy_settings")

    val rdd = cass_table.map(r => new Starter.DeviceEnergySettings(
      r.getString("siteid"),
      r.getString("nodeid"),
      r.getDouble("legacy_power"),
      r.getDouble("max_led_power")
    ))

    import JobSessions.spark.implicits._
    val DFdevicesettings = rdd.toDF()

    DFdevicesettings.createOrReplaceTempView("DFdevicesettings")

    var siteStmt = "na"
    if (Starter.inp_site == "ALL") siteStmt = " select distinct nodeid from DFdevicesettings order by nodeid "
    else siteStmt = "select distinct nodeid from DFdevicesettings where siteid = '" + Starter.inp_site + "' order by nodeid "

    val DFsiteNodes = JobSessions.spark.sql(siteStmt)
    val DFnodeGroup = Starter.pivotGroup(DFsiteNodes, 100, "String")

    //DFsiteNodes.createOrReplaceTempView("DFsiteNodes")

    Starter.rootLogger.info("Number of nodes to search for energy samples: " + DFsiteNodes.count() )

    import JobSessions.spark.implicits._
    JobSessions.spark.emptyDataset[Starter.DeviceSensorSamples].toDF().createOrReplaceTempView("allnodesdata")

    DFnodeGroup.collect().foreach(r => {

      import JobSessions.spark.implicits._

      val nodeidGrp = r.getString(0)
      val cass_table = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "device_sensor_samples")

      /*
              "and    date >= maxTimeuuid('" + Starter.getStringFromMicrosecs(from.toLong) + "') " +
              "and    date <  maxTimeuuid('" + Starter.getStringFromMicrosecs(until.toLong) + "') "
      */

      var stmt = ""

      if (Starter.inp_version == "R") {
        stmt = " nodeid in (" + nodeidGrp + ") " +
        " and    sensor in ('w','mw') " +
        " and    date >= maxTimeuuid('" + Starter.getStringFromMicrosecs((from.toLong) - (Starter.POWER_SAMPLING_INTERVAL_SEC*1000000)) + "') " +
        " and    date <  maxTimeuuid('" + Starter.getStringFromMicrosecs(until.toLong) + "') "
      }
      else {
        stmt = " nodeid in (" + nodeidGrp + ") " +
        " and    sensor in ('w','mw') " +
        " and    time >= " + ((from.toLong) - (Starter.POWER_SAMPLING_INTERVAL_SEC*1000000)).toString +
        " and    time <  " + until
      }

      val c1 = cass_table.where(stmt)

      val c2 = c1.limit(Starter.eventLimit.toLong)

      val rdd = c2.map(r => new Starter.DeviceSensorSamples(
        r.getString("nodeid"),
        r.getString("sensor"),
        //r.getString("date"),
        r.get[Option[String]]("date").getOrElse("na"),
        r.getLong("time"),
        r.getDouble("value")
      ))

      //Starter.rootLogger.info("Node (" + nodeid + ") with sensor sample count: " + rdd.count().toString)

      val df = rdd.toDF()

      val newdf = df.union(JobSessions.spark.sql("select * from allnodesdata"))
      newdf.createOrReplaceTempView("allnodesdata")
    })

    val DFalldata = JobSessions.spark.sql("select * from allnodesdata distribute by nodeid")

    Starter.rootLogger.info("Number of energy samples found: " + DFalldata.count().toString)

    DFalldata.sortWithinPartitions("time")
    DFalldata.createOrReplaceTempView("DFallData")

    /*
    DFjoinedData = JobSessions.spark.sql(
        " select a.nodeid, a.time, a.value, z.siteid, z.legacy_power, z.max_led_power, " +
        "        max(a.time) over (partition by a.nodeid) as maxtime, " +
        "        current_timestamp() as ts " +
        " from   DFallData a " +
        " JOIN   DFdevicesettings z ON a.nodeid = z.nodeid "
    )
    */

    DFjoinedData = JobSessions.spark.sql(
        " select a.nodeid, a.time, a.value, z.siteid, z.legacy_power, z.max_led_power, " +
        "        max(a.time) over (partition by a.nodeid) as maxtime, " +
        "        current_timestamp() as ts " +
        " from   ( " +
        "        select nodeid,time, " +
        "               (value-lag(value) over (partition by nodeid order by time)) " +
        "               / (time-lag(time) over (partition by nodeid order by time))  " +
        "               * 3600000000 " +
        "               as value " +
        "        from   DFallData " +
        "        ) a " +
        " JOIN   DFdevicesettings z ON a.nodeid = z.nodeid " +
        " where  a.value > 0 "
    )

    DFjoinedData.createOrReplaceTempView("DFjoinedData")

    return DFjoinedData
  }
}

object ProcessNodes {
  // Node level aggregation for the lowest level time bucket (TimeBucket.bucketSize), 1hr and day

  val rootLogger = Logger.getRootLogger

  def processData (iter: Iterator[Row], startDt: DateTime, untilDt: DateTime, POWER_SAMPLING_INTERVAL_SEC: Int, THRESHOLD_ZERO_POWER: Double ): Iterator[Row] = { // ilker add more parameters for closure issues

    /*
    Keeps track of time buckets and acccumulates its data, then writes the rolled-up values back to aggregation_energy_savings_node table.
    */

    val bucketSize = Starter.bucketSize
    val bucketAggType = (bucketSize/60000000).toString+"min"
    val timeHours = ((bucketSize.toDouble/1000000)/3600)
    val powerThreshold = THRESHOLD_ZERO_POWER

    var listBucketNode = ListBuffer[Row]()

    /*
    TODO:
    Start from the top of the hour, the most recent top of the hour before HWM. Re-processing some records each time, which will be fine from the
    functionality perspective, but we should consider performance cost vs. dealing with data transmission delays. Not sure if we need to worry
    about it right now.
    */

    /*
    TODO:
    Look into how we can synchronize the rollup job so that we minimize the need for re-processing. But we may still need to re-process most recent data
    if we want to handle data transfer delays/anomalies/data re-sends. I need to find out about such scenarios.
    */

    var legacyPower = 0.0d
    var ledPower = 0.0d
    var bucketSiteid = "na"
    var bucketNodeid = "na"

    var legacyConsumption = 0.0d
    var ledConsumption = 0.0d
    var actualConsumption = 0.0d

    var savLegacyLed = 0.0d
    var savLegacyActual = 0.0d

    var sampleCnt = 0
    // num of samples in bucket
    var avgSample = 0.0d

    // These are just for a more readable date/time values to help data consumption. They are not used in processing.
    var bucketStart = "na"
    // yyyy-MM-dd HH:mm
    var bucketEnd = "na"
    // yyyy-MM-dd HH:mm
    var bucketStartHr = "na"
    // yyyy-MM-dd HH
    var bucketStartDay = "na" // yyyy-MM-dd

    // Time bucket aggregations, accumulated values
    var startTime = 0L
    // epoch(microsecs)
    var endTime = 0L
    // epoch(microsecs)

    def writeToTables() = {
      /*
      Accumulates rolled-up values to be written back to a Cassandra tables for later consumption by APIs or other ad-hoc data analysis applications.
      */

      listBucketNode += Row(
        bucketSiteid, bucketNodeid, bucketAggType, startTime,
        actualConsumption, bucketEnd, endTime, ledConsumption, legacyConsumption,
        savLegacyActual, savLegacyLed, bucketStartDay,
        bucketStart, bucketStartHr, DateTime.now(DateTimeZone.UTC).getMillis
      )

      /*
      collection.saveToCassandra(cassKs, "aggregation_energy_savings_node",
        SomeColumns(
          "siteid", "nodeid", "aggregation_type", "starttime",
          "endtime", "legacy_energy_consumption", "led_energy_consumption", "actual_energy_consumption", "savings_legacy_vs_led", "savings_legacy_vs_actual",
          "startdt", "enddt", "starthr", "startday", "ts")
      )
      */

    }

    def newNode(dLast: DateTime, site: String, node: String, legacyP: Double, ledP: Double) = {
      /*
      New node detected. Initialize the time bucket.
      */

      bucketSiteid = site
      bucketNodeid = node
      legacyPower = legacyP
      ledPower = ledP

      startTime = dLast.withMinuteOfHour(0).getMillis * 1000 // start of the hr to handle data delays- in microsecs
      endTime = startTime + bucketSize

      legacyConsumption = 0.0d
      ledConsumption = 0.0d
      actualConsumption = 0.0d
      sampleCnt = 0
      avgSample = 0.0d

      savLegacyLed = 0.0d
      savLegacyActual = 0.0d

      bucketStart = (new DateTime(startTime / 1000, DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
      bucketEnd = (new DateTime(endTime / 1000, DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
      bucketStartHr = bucketStart.substring(0, 13)
      bucketStartDay = bucketStart.substring(0, 10)
    }

    def addEnergy(e: Double) = {
      /*
      Add actual energy consumption, and corresponding led / legacy consumption to current bucket.
      */

      if (e > powerThreshold) {
        // adding to the average in case of multiple samples
        actualConsumption = (((actualConsumption * sampleCnt) + e) / (sampleCnt + 1))

        // increase sample count so that we can add into the avg value
        sampleCnt += 1

        // populate led and leagy max energy as-if they are on full power when there is actual consumption
        legacyConsumption = legacyPower
        ledConsumption = ledPower
      }
    }

    def nextBucket(until: DateTime): Boolean = {
      /*
      Move the time bucket forward. Still the same node.
      */

      // Reached processing window-end. Do not go more.
      if (until.getMillis() * 1000 < endTime + bucketSize) return false

      // Prepare the new bucket
      startTime += bucketSize
      endTime += bucketSize
      bucketStart = (new DateTime(startTime / 1000, DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
      bucketEnd = (new DateTime(endTime / 1000, DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
      bucketStartHr = bucketStart.substring(0, 13)
      bucketStartDay = bucketStart.substring(0, 10)

      legacyConsumption = 0.0d
      ledConsumption = 0.0d
      actualConsumption = 0.0d
      sampleCnt = 0
      avgSample = 0.0d
      savLegacyLed = 0.0d
      savLegacyActual = 0.0d

      return true
    }

    def endBucket(pv: Double, v: Double, len: Long, r: Int) = {
      /*
      Data for the current bucket is exhausted. Finalize it now.
      */

      // if bucket empty, and in range of expected sampling then it means we have missing sample, so add the avg of the two neighbor samples
      if (sampleCnt == 0 && pv > 0.0d && v > 0.0d && len <= r*2)
        addEnergy((pv+v)/2)

      savLegacyActual = timeHours*(legacyConsumption - actualConsumption)
      savLegacyLed = timeHours*(legacyConsumption - ledConsumption)

      /*
      Starter.rootLogger.info("bucket end: ")
      Starter.rootLogger.info("site: " + bucketSiteid + " node: " + bucketNodeid + " start: " + bucketStart +
        " actual: " + actualConsumption.toString +
        " legacy: " + legacyConsumption.toString +
        " led: " + ledConsumption.toString
      )
      */

      writeToTables()
    }
    // end TimeBucket

    var siteid = "na"
    var nodeid = "na"
    var legacy = 0d
    var led = 0d

    var prevNode = "na"
    var prevTime = 0L
    var prevValue = 0d

    var time = 0L
    var maxtime = 0L
    var value = 0d
    var goodToGo = true

    val myList = iter.toList

    listBucketNode.clear()

    if (myList.size == 0) return listBucketNode.toIterator

    /*
    import JobSessions.spark.implicits._
    val df = JobSessions.spark.sql(
      "select nodeid, time, value, siteid, legacy_power, max_led_power, maxtime, ts " +
        " from DFjoinedData " +
        " order by nodeid, time")
    */

    myList.foreach(r=> {
      siteid = r.getString(3)
      nodeid = r.getString(0)
      legacy = r.getDouble(4)
      led = r.getDouble(5)

      time = r.getLong(1)
      maxtime = r.getLong(6)
      value = r.getDouble(2)


      //rootLogger.info(nodeid + " " + time.toString + " " + maxtime.toString + " " + value.toString)

      // New nodeid. This will always be the case for a partition right now. But we may choose to partition differently for better performance.
      if (prevNode != nodeid) {
        // initialize for the new node
        prevValue = 0d
        prevTime = 0L
        newNode(startDt, siteid, nodeid, legacy, led)
        goodToGo = true
      }

      if ((time >= endTime) && goodToGo) {
        // Move the time bucket(s) forward to create/end next bucket(s) until reaching to current record's timestamp.
        do {
          endBucket(prevValue, value, ((time-prevTime)/1000000), POWER_SAMPLING_INTERVAL_SEC)
          goodToGo = nextBucket(untilDt)
        }
        while ((goodToGo) && time >= endTime)
      }

      // If we are still in the processing time window, go for it. If not, records are effectively skipped.
      if (goodToGo) {
        if ((time >= startTime) && (time < endTime)) {
          // Current record falls into the current time bucket.
          addEnergy(value)
        }
      }

      /*
      End the buckets for the node until processing window-end is reached if this is the last sample for partition.
      */
      if (time == maxtime && nodeid != "na") {
        // End of data for a node; end the bucket(s) for the last node until reaching the processing window-end.
        goodToGo = true
        do {
          endBucket(0, 0, 0, 0) // 0s because we do not do missing sample treatment for the trailing end
          goodToGo = nextBucket(untilDt)
        }
        while ((goodToGo))

        /*
        val list = listBucketNode.toList
        val rdd = JobSessions.sc.makeRDD(list)
        val DFagg = JobSessions.spark.createDataFrame(rdd, Starter.StructEnergySavings)
        val newdf = DFagg.union(JobSessions.spark.sql("select * from aggnode"))
        newdf.createOrReplaceTempView("aggnode")
        listBucketNode.clear()
        */
      }

      /*
      So that I have access to previous record's critical values. This allows me to do a single pass on DataFrame. I am moving the record
      pointer (of DataFrame) and the time buckets simultaneously.
      */
      prevNode = nodeid
      prevTime = time
      prevValue = value
    }) // all samples for all nodes done

    return listBucketNode.toIterator
  }

  def run(startDt: DateTime, untilDt: DateTime, cassKeySpace: String, POWER_SAMPLING_INTERVAL_SEC: Int, THRESHOLD_ZERO_POWER: Double ) = {

    import JobSessions.spark.implicits._

    val timeHours = ((Starter.bucketSize.toDouble/1000000)/3600)

    val dfj = GetNodesData.apply(
      (Starter.dtLast.getMillis * 1000).toString,
      (Starter.rollupUntil.getMillis * 1000).toString
    )
    .repartition($"nodeid")
    .sortWithinPartitions("nodeid","time")

    val myResRDD = dfj.rdd.mapPartitions(iter => {
      processData(iter, startDt, untilDt, POWER_SAMPLING_INTERVAL_SEC, THRESHOLD_ZERO_POWER)
    })

    val DFagg = JobSessions.spark.createDataFrame(myResRDD, Starter.StructEnergySavings)
    DFagg.createOrReplaceTempView("aggnode")

    /*
    Node level time buckets
    */
    val DFaggspot = JobSessions.spark.sql("select * from aggnode")
    Starter.rootLogger.info("Node level bucket count: " + DFaggspot.count().toString)

    DFaggspot.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_energy_savings_node", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    val dfAggHr = JobSessions.spark.sql(
      " select siteid, nodeid, '1hr' as aggregation_type, starthr, startday, " +
      "        min(starttime) as starttime, " +
      "        avg(actual_energy_consumption) as actual_energy_consumption, " +
      "        max(enddt) as enddt, " +
      "        max(endtime) as endtime, " +
      "        avg(led_energy_consumption) as led_energy_consumption, " +
      "        avg(legacy_energy_consumption) as legacy_energy_consumption, " +
      "        sum(savings_legacy_vs_actual) as savings_legacy_vs_actual, " +
      "        sum(savings_legacy_vs_led) as savings_legacy_vs_led, " +
      "        min(startdt) as startdt, " +
      "        max(ts) as ts " +
      " from   aggnode " +
      " group  by siteid, nodeid, starthr, startday "
    )

    dfAggHr.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_energy_savings_node", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    val dfAggDay = JobSessions.spark.sql(
      " select siteid, nodeid, '1day' as aggregation_type, startday, " +
      "        min(starttime) as starttime, " +
      "        avg(actual_energy_consumption) as actual_energy_consumption, " +
      "        max(enddt) as enddt, " +
      "        max(endtime) as endtime, " +
      "        avg(led_energy_consumption) as led_energy_consumption, " +
      "        avg(legacy_energy_consumption) as legacy_energy_consumption, " +
      "        sum(savings_legacy_vs_actual) as savings_legacy_vs_actual, " +
      "        sum(savings_legacy_vs_led) as savings_legacy_vs_led, " +
      "        min(startdt) as startdt, " +
      "        min(starthr) as starthr, " +
      "        max(ts) as ts " +
      " from   aggnode " +
      " group  by siteid, nodeid, startday "
    )

    dfAggDay.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_energy_savings_node", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    /*
    Site level time buckets
    */

    val dfAggSite = JobSessions.spark.sql(
      " select siteid, '15min' as aggregation_type, startdt, starthr, startday, " +
      "        min(starttime) as starttime, " +
      "        sum(actual_energy_consumption) as actual_energy_consumption, " +
      "        max(enddt) as enddt, " +
      "        max(endtime) as endtime, " +
      "        sum(led_energy_consumption) as led_energy_consumption, " +
      "        sum(legacy_energy_consumption) as legacy_energy_consumption, " +
      "        sum(savings_legacy_vs_actual) as savings_legacy_vs_actual, " +
      "        sum(savings_legacy_vs_led) as savings_legacy_vs_led, " +
      "        max(ts) as ts " +
      " from   aggnode " +
      " group  by siteid, startdt, starthr, startday "
    )

    dfAggSite.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_energy_savings_site", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    val dfAggHrSite = JobSessions.spark.sql(
      " select siteid, '1hr' as aggregation_type, starthr, startday, " +
      "        min(starttime) as starttime, " +
      "        sum(actual_energy_consumption) * " + timeHours.toString + " as actual_energy_consumption, " +
      "        max(enddt) as enddt, " +
      "        max(endtime) as endtime, " +
      "        sum(led_energy_consumption) * " + timeHours.toString + "  as led_energy_consumption, " +
      "        sum(legacy_energy_consumption) * " + timeHours.toString + " as legacy_energy_consumption, " +
      "        sum(savings_legacy_vs_actual) as savings_legacy_vs_actual, " +
      "        sum(savings_legacy_vs_led) as savings_legacy_vs_led, " +
      "        min(startdt) as startdt, " +
      "        max(ts) as ts " +
      " from   aggnode " +
      " group  by siteid, starthr, startday "
    )

    dfAggHrSite.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_energy_savings_site", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    val dfAggDaySite = JobSessions.spark.sql(
      " select siteid, '1day' as aggregation_type, startday, " +
      "        min(starttime) as starttime, " +
      "        sum(actual_energy_consumption) * " + timeHours.toString + " as actual_energy_consumption, " +
      "        max(enddt) as enddt, " +
      "        max(endtime) as endtime, " +
      "        sum(led_energy_consumption) * " + timeHours.toString + "  as led_energy_consumption, " +
      "        sum(legacy_energy_consumption) * " + timeHours.toString + " as legacy_energy_consumption, " +
      "        sum(savings_legacy_vs_actual) as savings_legacy_vs_actual, " +
      "        sum(savings_legacy_vs_led) as savings_legacy_vs_led, " +
      "        min(startdt) as startdt, " +
      "        min(starthr) as starthr, " +
      "        max(ts) as ts " +
      " from   aggnode " +
      " group  by siteid, startday "
    )

    dfAggDaySite.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_energy_savings_site", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

  }

}


object old_GetDataForNode {
  /*
  All data for a given nodeid in the processing window is collected into DataFrame, later to be aggregated and rolled-up to time bucket level (resolution)
  */

  def apply(nodeid: String, from: String, until: String, cK: String): RDD[(String,Iterable[Starter.DeviceSensorSamples])] = {

    val cass_table = JobSessions.sc.cassandraTable(cK, "device_sensor_samples")

    val c1 = cass_table.where(
      "         nodeid = '" + nodeid + "' " +
        "and    sensor = '" + Starter.POWER_PARAMETER + "' " +
        //"and    sensor = 'p' " +
        "and    date >= maxTimeuuid('" + Starter.getStringFromMicrosecs(from.toLong) + "') " +
        "and    date <  maxTimeuuid('" + Starter.getStringFromMicrosecs(until.toLong) + "') "
    )

    val c2 = c1.limit(Starter.eventLimit.toLong)

    val rdd = c2.map(r => new Starter.DeviceSensorSamples(
      r.getString("nodeid"),
      r.getString("sensor"),
      r.getString("date"),
      r.getLong("time"),
      r.getDouble("value")
    ))

    Starter.rootLogger.info("Node (" + nodeid + ") with sensor sample count: " + rdd.count().toString)

    val kv = rdd.map(r=>{
      Tuple2(r.nodeid,r)
    })
    .groupByKey()
   //   .groupByKey(2) ilker

    return kv
  }
}

/*
object TimeBucket {

  /*
  Keeps track of time buckets and acccumulates its data, then writes the rolled-up values back to aggregation_energy_savings_node table.
  */

  val bucketSize = Starter.bucketSize
  val bucketAggType = (bucketSize/60000000).toString+"min"
  val timeHours = ((bucketSize.toDouble/1000000)/3600)
  val powerThreshold = Starter.THRESHOLD_ZERO_POWER

  var listBucketNode = ListBuffer[Row]()

  /*
  TODO:
  Start from the top of the hour, the most recent top of the hour before HWM. Re-processing some records each time, which will be fine from the
  functionality perspective, but we should consider performance cost vs. dealing with data transmission delays. Not sure if we need to worry
  about it right now.
  */

  /*
  TODO:
  Look into how we can synchronize the rollup job so that we minimize the need for re-processing. But we may still need to re-process most recent data
  if we want to handle data transfer delays/anomalies/data re-sends. I need to find out about such scenarios.
  */

  var legacyPower = 0.0d
  var ledPower = 0.0d
  var bucketSiteid = "na"
  var bucketNodeid = "na"

  var legacyConsumption = 0.0d
  var ledConsumption = 0.0d
  var actualConsumption = 0.0d

  var savLegacyLed = 0.0d
  var savLegacyActual = 0.0d

  var sampleCnt = 0
  // num of samples in bucket
  var avgSample = 0.0d

  // These are just for a more readable date/time values to help data consumption. They are not used in processing.
  var bucketStart = "na"
  // yyyy-MM-dd HH:mm
  var bucketEnd = "na"
  // yyyy-MM-dd HH:mm
  var bucketStartHr = "na"
  // yyyy-MM-dd HH
  var bucketStartDay = "na" // yyyy-MM-dd

  // Time bucket aggregations, accumulated values
  var startTime = 0L
  // epoch(microsecs)
  var endTime = 0L
  // epoch(microsecs)

  def writeToTables() = {
    /*
    Accumulates rolled-up values to be written back to a Cassandra tables for later consumption by APIs or other ad-hoc data analysis applications.
    */

    listBucketNode += Row(
      bucketSiteid, bucketNodeid, bucketAggType, startTime,
      actualConsumption, bucketEnd, endTime, ledConsumption, legacyConsumption,
      savLegacyActual, savLegacyLed, bucketStartDay,
      bucketStart, bucketStartHr, DateTime.now(DateTimeZone.UTC).getMillis
    )

    /*
    collection.saveToCassandra(cassKs, "aggregation_energy_savings_node",
      SomeColumns(
        "siteid", "nodeid", "aggregation_type", "starttime",
        "endtime", "legacy_energy_consumption", "led_energy_consumption", "actual_energy_consumption", "savings_legacy_vs_led", "savings_legacy_vs_actual",
        "startdt", "enddt", "starthr", "startday", "ts")
    )
    */

  }

  def newNode(dLast: DateTime, site: String, node: String, legacyP: Double, ledP: Double) = {
    /*
    New node detected. Initialize the time bucket.
    */

    bucketSiteid = site
    bucketNodeid = node
    legacyPower = legacyP
    ledPower = ledP

    startTime = dLast.withMinuteOfHour(0).getMillis * 1000 // start of the hr to handle data delays- in microsecs
    endTime = startTime + bucketSize

    legacyConsumption = 0.0d
    ledConsumption = 0.0d
    actualConsumption = 0.0d
    sampleCnt = 0
    avgSample = 0.0d

    savLegacyLed = 0.0d
    savLegacyActual = 0.0d

    bucketStart = (new DateTime(startTime / 1000, DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
    bucketEnd = (new DateTime(endTime / 1000, DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
    bucketStartHr = bucketStart.substring(0, 13)
    bucketStartDay = bucketStart.substring(0, 10)
  }

  def addEnergy(e: Double) = {
    /*
    Add actual energy consumption, and corresponding led / legacy consumption to current bucket.
    */

    if (e > powerThreshold) {
      // adding to the average in case of multiple samples
      actualConsumption = (((actualConsumption * sampleCnt) + e) / (sampleCnt + 1))

      // increase sample count so that we can add into the avg value
      sampleCnt += 1

      // populate led and leagy max energy as-if they are on full power when there is actual consumption
      legacyConsumption = legacyPower
      ledConsumption = ledPower
    }
  }

  def nextBucket(until: DateTime): Boolean = {
    /*
    Move the time bucket forward. Still the same node.
    */

    // Reached processing window-end. Do not go more.
    if (until.getMillis() * 1000 < endTime + bucketSize) return false

    // Prepare the new bucket
    startTime += bucketSize
    endTime += bucketSize
    bucketStart = (new DateTime(startTime / 1000, DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
    bucketEnd = (new DateTime(endTime / 1000, DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
    bucketStartHr = bucketStart.substring(0, 13)
    bucketStartDay = bucketStart.substring(0, 10)

    legacyConsumption = 0.0d
    ledConsumption = 0.0d
    actualConsumption = 0.0d
    sampleCnt = 0
    avgSample = 0.0d
    savLegacyLed = 0.0d
    savLegacyActual = 0.0d

    return true
  }

  def endBucket(pv: Double, v: Double, len: Long, r: Int) = {
    /*
    Data for the current bucket is exhausted. Finalize it now.
    */

    // if bucket empty, and in range of expected sampling then it means we have missing sample, so add the avg of the two neighbor samples
    if (sampleCnt == 0 && pv > 0.0d && v > 0.0d && len <= r*2)
      addEnergy((pv+v)/2)

    savLegacyActual = timeHours*(legacyConsumption - actualConsumption)
    savLegacyLed = timeHours*(legacyConsumption - ledConsumption)

    /*
    Starter.rootLogger.info("bucket end: ")
    Starter.rootLogger.info("site: " + bucketSiteid + " node: " + bucketNodeid + " start: " + bucketStart +
      " actual: " + actualConsumption.toString +
      " legacy: " + legacyConsumption.toString +
      " led: " + ledConsumption.toString
    )
    */

    writeToTables()
  }

}
*/


