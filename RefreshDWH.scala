package com.sensity.dwh

import java.util.UUID

import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.json4s.jackson.JsonMethods.{compact, parse, render}
//import com.sensity.dwh.ProcessDeviceAlarms.DeviceAlarms
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.joda.time._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer


object Starter {

  case class AggregationControl(typ: String, key: String, value: String)

  var SparkAppName = "refresh-dwh"
  val CLEANUP_DAYS = 3 // Truncate partitions in staging if older than this setting
  val STG_PREFIX = "stg_"
  //val DWH_PREFIX = "dwh_"
  val DWH_PREFIX = "fs_"
  val SV_PREFIX = "sv_"
  var DWH_SCHEMA = "default"
  var HWM_LIMIT_MINS = "na"
  val filterPartitionToday = DateTime.now(DateTimeZone.UTC).toString("yyyy-MM-dd")
  val filterPartitionYesterday = DateTime.now(DateTimeZone.UTC).minusDays(1).toString("yyyy-MM-dd")
  val filterPartitionB = DateTime.now(DateTimeZone.UTC).minusDays(2).toString("yyyy-MM-dd")
  val flushCount = 20L

  // Arguments
  var inp_cassIP = "52.40.52.243" // Use QA Cassandra instance as default. Must be passed for production runs.
  var inp_neoIP = "52.24.76.85"
  var inp_runMode = "TEST_LOCAL"  // Run in local[*] mode as default. Must be passed for production runs.
  var inp_runLevel = "111111111" // Run level selection. See RUN TABLE below. Default is run all.
  var inp_eventRecordLimit = "0" // If passed, will limit the number of event records to be processed. Should be used for testing only.
  var inp_DWHLocation = "NONE"

  var inp_sparkCleanerTTL = "3600"
  var inp_cassKeyspace = "farallones"
  var inp_cassPort = "9042"
  var inp_neoPort = "7474"

  /*
  runLevel flags are used to turn on/off specific data type's refresh. 1 is on, 0 is of.
  RUN TABLE
   idx  Data Type
   ---  ---------------------------
    1   Neo4j Data for configurations
    2   Device Alarms
    3   Device Sensor Samples
    4   Aggregation Parking Spot, Parking Spot Historic
    5   Aggregation Traffic Events and Event Counts Update
    6   Parking Zone Status
    7   Energy Savings
    8   Parking Spot Current
    9   Connection Status (2 tables)
  */

  val rootLogger = Logger.getRootLogger
  val sparkLogging = Logger.getLogger("org.apache.spark")
  val cassandraLogging = Logger.getLogger("com.datastax")

  var rowLimit = 100000000L
  var inp_startFromDT = "NONE" // In UTC yyyy-MM-dd HH:mm format. If passed, will start the rollup at the most recent top of the hour of this value. If
  // not passed, HWM will be used. Parameter should be used for testing only.
  var inp_processUntilDT = "NONE" // In UTC yyyy-MM-dd HH:mm format. If passed, will end the rollup at the most recent top of the hour of this value. If
  // not passed, "now" will be used. Parameter should be used for testing only.

  var dtLast = DateTime.now(DateTimeZone.UTC)
  val timeEnd = DateTime.now(DateTimeZone.UTC)
  var rollupUntil = timeEnd.withMinuteOfHour(0)

  def main(args: Array[String]) {

    receiveParameters(args)

    Starter.rootLogger.info("Initializing...")
    InitCassandra.apply()

    if (inp_startFromDT == "NONE") {
      dtLast = HighWaterMark.get() // Get the most recent rollup run time
      if (HWM_LIMIT_MINS != "na" && dtLast.plusMinutes(HWM_LIMIT_MINS.toInt).isBeforeNow) {
        dtLast = timeEnd.minusMinutes(HWM_LIMIT_MINS.toInt)
      }
    }
    else
      dtLast = getDateFrom(inp_startFromDT) // Rollup start time passed as input

    if (inp_processUntilDT == "NONE")
      rollupUntil = timeEnd.withSecondOfMinute(0) // Until now
    else
      rollupUntil = getDateFrom(inp_processUntilDT).withSecondOfMinute(0) // Rollup end time passed as input

    rootLogger.info("Cassandra Keyspace : " + inp_cassKeyspace)
    rootLogger.info("Cassandra IP       : " + inp_cassIP)
    rootLogger.info("Neo4j IP           : " + inp_neoIP)
    rootLogger.info("Run Mode           : " + inp_runMode)
    rootLogger.info("Run Level          : " + inp_runLevel)
    rootLogger.info("Record Limit       : " + inp_eventRecordLimit)
    rootLogger.info("Start Time         : " + inp_startFromDT)
    rootLogger.info("End Time           : " + inp_processUntilDT)
    rootLogger.info("DWH Location       : " + inp_DWHLocation)
    rootLogger.info("Spark Cleaner      : " + inp_sparkCleanerTTL)
    rootLogger.info("Cassandra port     : " + inp_cassPort)
    rootLogger.info("Neo4j port         : " + inp_neoPort)
    rootLogger.info("                      ...To... ")
    rootLogger.info("DWH Schema         : " + DWH_SCHEMA)

    rootLogger.info("Data refresh started for data after " + dtLast.toString("yyyy-MM-dd HH:mm") +
      " until " + rollupUntil.toString("yyyy-MM-dd HH:mm") )

    if (inp_runLevel == "001000000_") HighWaterMark.write()

    val dtFromWithMinOffset = dtLast.minusMinutes(1)
    val dtFromWith5MinOffset = dtLast.minusMinutes(5)
    val dtFromWith15MinOffset = dtLast.minusMinutes(15)
    val dtFromWithHrOffset = dtLast.minusHours(1)
    val dtFromWithDayOffset = dtLast.minusDays(1)

    if (inp_runLevel.substring(0,1) == "1") {
      // Refresh Config Data From Neo4j
      Starter.rootLogger.info("Processing Neo4j Data...")
      ProcessCopyNeo.run
    }
    if (inp_runLevel.substring(1,2) == "1") {
      // Take alarm data with a minute offset
      Starter.rootLogger.info("Processing device_alarms from Cassandra...")
      ProcessDeviceAlarms.run(dtFromWithMinOffset, rollupUntil, inp_cassKeyspace)
    }
    if (inp_runLevel.substring(2,3) == "1") {
      // Take sensor data with a minute offset
      Starter.rootLogger.info("Processing device_sensor_samples from Cassandra...")
      ProcessDeviceSensorSamples.run(dtFromWithMinOffset, rollupUntil, inp_cassKeyspace)
    }
    if (inp_runLevel.substring(3,4) == "1") {
      // Take  parking spot historic data with a few minutes offset
      Starter.rootLogger.info("Processing parking_spot_historic from Cassandra...")
      ProcessParkingHistoric.run(dtFromWith5MinOffset, rollupUntil, inp_cassKeyspace)

      // Take parking aggregation data with an hour offset
      Starter.rootLogger.info("Processing aggregation_parking_spot from Cassandra...")
      ProcessAggregationParkingSpot.run(dtFromWithHrOffset, rollupUntil, inp_cassKeyspace)
    }
    if (inp_runLevel.substring(4,5) == "1") {
      // Take traffic status, and update the count at traffic current status
      Starter.rootLogger.info("Processing traffic_status from Cassandra...")
      //ProcessTrafficStatus.run(dtLast.withSecondOfMinute(0), rollupUntil, inp_cassKeyspace)
      ProcessTrafficStatus.run(dtFromWith15MinOffset, rollupUntil, inp_cassKeyspace)

      // Take traffic aggregation data with a day offset
      Starter.rootLogger.info("Processing aggregation_traffic_events from Cassandra...")
      ProcessAggregationTrafficEvents.run(dtFromWithHrOffset, rollupUntil, inp_cassKeyspace)
    }
    if (inp_runLevel.substring(5,6) == "1") {
      // Refresh Parking Groups from cassandra parking_zone_status table
      Starter.rootLogger.info("Processing parking_zone_status from Cassandra...")
      ProcessParkingZoneStatus.run(inp_cassKeyspace)
    }
    if (inp_runLevel.substring(6,7) == "1") {
      // Take energy savings aggregation data from cassandra aggregation_energy_savings_node table with a day offset
      Starter.rootLogger.info("Processing aggregation_energy_savings_node from Cassandra...")
      ProcessEnergySavings.run(dtFromWithDayOffset, rollupUntil, inp_cassKeyspace)
    }
    if (inp_runLevel.substring(7,8) == "1") {
      // Refresh parking spot data from cassandra parking_spot_current table
      Starter.rootLogger.info("Processing parking_spot_current from Cassandra...")
      ProcessParkingSpotCurrent.run(inp_cassKeyspace)
    }
    if (inp_runLevel.substring(8,9) == "1") {
      // Take connection status data with a minute offset
      Starter.rootLogger.info("Processing connection_status from Cassandra...")
      ProcessConnectionStatus.run(dtFromWithMinOffset, rollupUntil, inp_cassKeyspace)

      // Take aggregation connection node data with an hour offset
      Starter.rootLogger.info("Processing aggregation_connection_node from Cassandra...")
      ProcessAggregationConnectionNode.run(dtFromWithHrOffset, rollupUntil, inp_cassKeyspace)
    }

    if (inp_runLevel != "001000000_") HighWaterMark.write()

    rootLogger.info("Copy Completed")
    JobSessions.sc.stop()
  }

  def CleanupWarehouse(tableName: String) = {

    JobSessions.spark.sql(
      " create table if not exists " + Starter.SV_PREFIX+tableName + " as select * from " + Starter.DWH_PREFIX+tableName +
        " where  startday = 'na' "
    )
    JobSessions.spark.sql(
      " insert overwrite table " + Starter.SV_PREFIX+tableName + " select * from " + Starter.DWH_PREFIX+tableName +
        " where  startday in ('" + Starter.filterPartitionToday + "', '" + Starter.filterPartitionYesterday + "') "
    )

    val df= JobSessions.spark.sql(
      " select distinct startday as partitionName " +
        " from " + Starter.DWH_PREFIX+tableName +
        " where  startday in ('" + Starter.filterPartitionToday + "', '" + Starter.filterPartitionYesterday + "') "
    )
    df.collect().foreach(r=> {
      JobSessions.spark.sql(
        " truncate table " + Starter.DWH_PREFIX+tableName + " PARTITION (startday = '" + r.getString(0) + "') "
      )
    })

  }

  def CleanupWarehouseRR(tableName: String, from:String, to:String) = {

    JobSessions.spark.sql(
      " create table if not exists " + Starter.SV_PREFIX+tableName + " as select * from " + Starter.DWH_PREFIX+tableName +
        " where  startday = 'na' "
    )
    JobSessions.spark.sql(
      " insert overwrite table " + Starter.SV_PREFIX+tableName + " select * from " + Starter.DWH_PREFIX+tableName +
        " where  startday between '" + from + "' and '" + to + "' "
    )

    val df= JobSessions.spark.sql(
      " select distinct startday as partitionName " +
        " from " + Starter.DWH_PREFIX+tableName +
        " where  startday between '" + from + "' and '" + to + "' "
    )
    df.collect().foreach(r=> {
      JobSessions.spark.sql(
        " truncate table " + Starter.DWH_PREFIX+tableName + " PARTITION (startday = '" + r.getString(0) + "') "
      )
    })

  }

  def CleanupWarehouseUTC(tableName: String) = {

    JobSessions.spark.sql(
      " create table if not exists " + Starter.SV_PREFIX+tableName + " as select * from " + Starter.DWH_PREFIX+tableName +
        " where  startday = 'na' "
    )
    JobSessions.spark.sql(
      " insert overwrite table " + Starter.SV_PREFIX+tableName + " select * from " + Starter.DWH_PREFIX+tableName +
        " where  startday in ('" + Starter.filterPartitionToday + "') "
    )
    val df= JobSessions.spark.sql(
      " select distinct startday as partitionName " +
        " from " + Starter.DWH_PREFIX+tableName +
        " where  startday in ('" + Starter.filterPartitionToday + "') "
    )
    df.collect().foreach(r=> {
      JobSessions.spark.sql(
        " truncate table " + Starter.DWH_PREFIX+tableName + " PARTITION (startday = '" + r.getString(0) + "') "
      )
    })
  }

  def CleanupWarehouseUTCnoSV(tableName: String) = {

    JobSessions.spark.sql(
      " create table if not exists " + Starter.SV_PREFIX+tableName + " as select * from " + Starter.DWH_PREFIX+tableName +
        " where  startday = 'na' "
    )
    val df= JobSessions.spark.sql(
      " select distinct startday as partitionName " +
        " from " + Starter.DWH_PREFIX+tableName +
        " where  startday in ('" + Starter.filterPartitionToday + "') "
    )
    df.collect().foreach(r=> {
      JobSessions.spark.sql(
        " truncate table " + Starter.DWH_PREFIX+tableName + " PARTITION (startday = '" + r.getString(0) + "') "
      )
    })
  }

  def CleanupSave(tableName: String) = {
    JobSessions.spark.sql(
      " truncate table " + Starter.SV_PREFIX+tableName
    )
  }

  def CleanupStaging(tableName: String) = {
    val filterPartition = DateTime.now(DateTimeZone.UTC).minusDays(Starter.CLEANUP_DAYS).toString("yyyy-MM-dd")

    val df= JobSessions.spark.sql(
      " select distinct startday as partitionName " +
        " from " + Starter.STG_PREFIX+tableName +
        " where  startday < '" + filterPartition + "' "
    )
    df.collect().foreach(r=> {
      JobSessions.spark.sql(
        " alter table " + Starter.STG_PREFIX+tableName + " drop PARTITION (startday = '" + r.getString(0) + "') "
      )
    })
  }

  def CleanupStagingRR(tableName: String, from:String, to:String) = {

    val df= JobSessions.spark.sql(
      " select distinct startday as partitionName " +
        " from " + Starter.STG_PREFIX+tableName +
        " where  startday between '" + from + "' and '" + to + "' "
    )
    df.collect().foreach(r=> {
      JobSessions.spark.sql(
        " alter table " + Starter.STG_PREFIX+tableName + " drop PARTITION (startday = '" + r.getString(0) + "') "
      )
    })
  }


  def receiveParameters(args: Array[String]) = {
    // Process Args
    if (args.length>0) inp_cassKeyspace = args(0)
    if (args.length>1) inp_cassIP = args(1)
    if (args.length>2) inp_neoIP = args(2)
    if (args.length>3) inp_runMode = args(3)
    if (args.length>4) inp_runLevel = args(4)
    if (args.length>5) inp_eventRecordLimit = args(5)
    if (args.length>6) inp_startFromDT = args(6)
    if (args.length>7) inp_processUntilDT = args(7)
    if (args.length>8) inp_DWHLocation = args(8)
    if (args.length>9) inp_sparkCleanerTTL = args(9)
    if (args.length>10) inp_cassPort = args(10)
    if (args.length>11) inp_neoPort = args(11)

    System.setProperty("runMode",inp_runMode)

    if (inp_runMode == "TEST_LOCAL" || inp_runMode == "TEST_YARN" ) {
      rootLogger.setLevel(Level.INFO)
      sparkLogging.setLevel(Level.ERROR)
      cassandraLogging.setLevel(Level.ERROR)
    }

    if (inp_eventRecordLimit != "0") rowLimit = inp_eventRecordLimit.toLong
    SparkAppName = "refresh-dwh-" + inp_runLevel

    var pVal = "na"
    pVal = Parameters.get("schema")
    if (pVal != "na") DWH_SCHEMA = pVal
    else {
      if (inp_cassKeyspace.length>10 && inp_cassKeyspace.substring(0,10) == "farallones") {
        DWH_SCHEMA = "dwh_" + inp_cassKeyspace.substring(10)
      }
    }

    JobSessions.spark.sql("create database if not exists " + DWH_SCHEMA)
    JobSessions.spark.catalog.setCurrentDatabase(DWH_SCHEMA)

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

    val stmt =
      " insert into " +  Starter.inp_cassKeyspace + ".aggregation_control(type, key, val) " +
        " values ('refreshDWH-" + Starter.inp_runLevel + "','last_refresh_min','" +
        DateTime.now(DateTimeZone.UTC).minusMinutes(60).toString("yyyy-MM-dd HH:mm") + "') if not exists "

    cassSession.execute(stmt)

    cassSession.close()
  }

}


object JobSessions {

  Logger.getRootLogger.info("Connecting to Sessions...")

  val CassandraSeed = Starter.inp_cassIP
  val KeySpace = Starter.inp_cassKeyspace
  val CassandraPort = Starter.inp_cassPort

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraSeed)
    .set("spark.cassandra.connection.port", CassandraPort)
    .set("spark.cassandra.input.consistency.level", "ONE")
    .set("spark.cassandra.output.consistency.level", "ONE") // ONE
    .set("spark.cleaner.ttl", Starter.inp_sparkCleanerTTL)
    .setAppName(Starter.SparkAppName)

  if (Starter.inp_runMode == "TEST_LOCAL" || Starter.inp_runMode == "PROD_LOCAL" ) conf.setMaster("local[1]")

  val spark = SparkSession
    .builder()
    .config(conf)
    //.config("spark.sql.warehouse.dir","/tmp/spark-warehouse")
    .enableHiveSupport()
    .getOrCreate()

  val sc = spark.sparkContext

}


object Parameters {
  /*
  Energy Savings algorithm parameters
  */

  def get(pName:String) :String = {

    var pVal = "na"

    val cass_table = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "aggregation_control")
    val c1 = cass_table.where(
      "       type = 'refreshDWH' " +
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

  def get(): DateTime = {

    val cass_table = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "aggregation_control")
    val c1 = cass_table.where(
      "       type = 'refreshDWH-" + Starter.inp_runLevel + "' " +
        "and  key = 'last_refresh_min' "
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

    if (Starter.inp_eventRecordLimit == "0" && Starter.inp_startFromDT == "NONE" && Starter.inp_processUntilDT == "NONE") {
      /*
      Writes high-water-mark for the next instance of roll-up.
      */
      Starter.rootLogger.info("Adjusting HWM...")
      val collection = JobSessions.sc.parallelize(Seq((
        "refreshDWH-" + Starter.inp_runLevel, "last_refresh_min", Starter.timeEnd.toString("yyyy-MM-dd HH:mm"))
      ))
      collection.saveToCassandra(Starter.inp_cassKeyspace, "aggregation_control",
        SomeColumns(
          "type", "key", "val")
      )
      Starter.rootLogger.info("HWM = " + Starter.timeEnd.toString("yyyy-MM-dd HH:mm"))
    }
    else {
      Starter.rootLogger.info("HWM not changed.")
    }
  }

}


object GetTrafficNodes {
  case class NodeList(nodeid: String)

  def apply(from: String): DataFrame = {

    import scala.collection.convert.wrapAll._
    import JobSessions.spark.implicits._

    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

    val stmt =
      " select distinct nodeid " +
        " from " + Starter.inp_cassKeyspace + ".traffic_status "
        //" where time >= " + from + " "
        //" allow filtering "

    val nodelist = cassSession.execute(stmt).all().toList

    val rdd = nodelist.map(r => new NodeList(
      r.getString(0)
    ))

    val df = rdd
      .toDF()
      .distinct()

    cassSession.close()

    return df
  }
}


object ProcessTrafficStatus {

  case class TrafficStatus(nodeid: String, time: Long,
                           active: Boolean,
                           channel: Int, count: Long, data: String, eventtype: String, eventname: String, orgid: String, siteid: String,
                           trafficdetectioneventid: String, updated: String
                           ,tags: Array[String]
                          )

  val tableName = "traffic_status"
  val rootLogger = Logger.getRootLogger

  def run(startDt: DateTime, untilDt: DateTime, cassKeySpace: String) = {

    import JobSessions.spark.implicits._

    CreateDWHTable

    JobSessions.spark.emptyDataset[TrafficStatus].toDF().createOrReplaceTempView("allTSdata")
    JobSessions.spark.emptyDataset[TrafficStatus].toDF().createOrReplaceTempView("allTSEmpty")

    val from = (startDt.getMillis * 1000).toString
    val until = (untilDt.getMillis * 1000).toString

    val DFnodes = GetTrafficNodes.apply(from)
    val DFnodeGroup = Starter.pivotGroup(DFnodes, 20, "String") // 200 scale

    DFnodeGroup.collect().foreach(r=> {

      import scala.collection.convert.wrapAll._
      val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

      var nodeidGrp = r.getString(0)

      val stmt =
        " select nodeid, time, active, channel, count, data, type, name, orgid, siteid, tags, trafficdetectioneventid, updated " +
          " from " + Starter.inp_cassKeyspace + "." + "traffic_status" +
          " where  nodeid in (" + nodeidGrp + ") " +
          " and    time >= " + from + " " +
          " and    time <  " + until + " "
      " limit " + Starter.rowLimit.toString +
        " allow  filtering "

      val cassData = cassSession.execute(stmt).all().toList

      val rdd = cassData.map(r => new TrafficStatus(
        r.getString("nodeid"),
        r.getLong("time"),
        r.getBool("active"),
        r.getInt("channel"),
        r.getLong("count"),
        r.getString("data"),
        r.getString("type"),
        r.getString("name"),
        r.getString("orgid"),
        r.getString("siteid"),
        r.getString("trafficdetectioneventid"),
        r.getUUID("updated").toString,
        null
      ))

      val df = rdd.toDF()

      InsertToSTG(df)
      saveData(df, cassKeySpace)

      cassSession.close()
    })

    Starter.CleanupStaging(tableName) //Starter.CleanupStagingRR(tableName,"2017-12-13", "2017-12-15")
    Starter.CleanupWarehouse(tableName) //Starter.CleanupWarehouseRR(tableName,"2017-12-13", "2017-12-15")
    UpdateDWH //UpdateDWHRR("2017-12-13", "2017-12-15")
    Starter.CleanupSave(tableName)
  }


  def CreateDWHTable = {

    val colList =
      " nodeid string, " +
        " time bigint, " +
        " active boolean, " +
        " channel int, " +
        " count bigint, " +
        " data string, " +
        " type string, " +
        " name string, " +
        " orgid string, " +
        " siteid string, " +
        " trafficdetectioneventid string, " +
        " updated string, " +
        " tags array<string>, " +
        " ts bigint "

    JobSessions.spark.sql(
      " create table if not exists " + Starter.STG_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        //" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
        " STORED AS ORC "
    )

    JobSessions.spark.sql(
      " create table if not exists " + Starter.DWH_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        //" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
        " STORED AS ORC "
    )
  }

  def InsertToSTG (df: DataFrame) ={
    df.createOrReplaceTempView("stgTrafficStatus")
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.STG_PREFIX+tableName + " partition (startday) " +
        " select " +
        " nodeid, " +
        " time, " +
        " active, " +
        " channel, " +
        " count, " +
        " data, " +
        " eventtype as type, " +
        " eventname as name, " +
        " orgid, " +
        " siteid, " +
        " trafficdetectioneventid, " +
        " updated, " +
        " tags, " +
        " unix_timestamp() as ts, " +
        " substr(from_unixtime(cast(time/1000000 as bigint)), 1, 10) as startday " +
        " from stgTrafficStatus "
    )
  }

  def UpdateDWH ={
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.DWH_PREFIX+tableName + " partition (startday) " +
        " select  nodeid,time,active,channel,count,data,type,name,orgid,siteid,trafficdetectioneventid,updated,tags, ts, startday " +
        " from " +
        " ( " +
        " select " +
        " nodeid, " +
        " time, " +
        " active, " +
        " channel, " +
        " count, " +
        " data, " +
        " type, " +
        " name, " +
        " orgid, " +
        " siteid, " +
        " trafficdetectioneventid, " +
        " updated, " +
        " tags, " +
        " max(ts) over (partition by nodeid, time) as maxts, " +
        " ts, " +
        " startday " +
        " from " + Starter.STG_PREFIX+tableName +
        " where startday in ('" + Starter.filterPartitionB + "', '" + Starter.filterPartitionYesterday + "', '" + Starter.filterPartitionToday + "') " +
        " ) a " +
        " where a.ts = a.maxts "
    )
  }

  def UpdateDWHRR (from:String, to:String) ={
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.DWH_PREFIX+tableName + " partition (startday) " +
        " select  nodeid,time,active,channel,count,data,type,name,orgid,siteid,trafficdetectioneventid,updated,tags, ts, startday " +
        " from " +
        " ( " +
        " select " +
        " nodeid, " +
        " time, " +
        " active, " +
        " channel, " +
        " count, " +
        " data, " +
        " type, " +
        " name, " +
        " orgid, " +
        " siteid, " +
        " trafficdetectioneventid, " +
        " updated, " +
        " tags, " +
        " max(ts) over (partition by nodeid, time) as maxts, " +
        " ts, " +
        " startday " +
        " from " + Starter.STG_PREFIX+tableName +
        " where startday between '" + from + "' and '" + to + "' " +
        " ) a " +
        " where a.ts = a.maxts "
    )
  }

  def UpdateDWHUTC ={
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.DWH_PREFIX+tableName + " partition (startday) " +
        " select  nodeid,time,active,channel,count,data,type,name,orgid,siteid,trafficdetectioneventid,updated,tags, ts, startday " +
        " from " +
        " ( " +
        " select " +
        " nodeid, " +
        " time, " +
        " active, " +
        " channel, " +
        " count, " +
        " data, " +
        " type, " +
        " name, " +
        " orgid, " +
        " siteid, " +
        " trafficdetectioneventid, " +
        " updated, " +
        " tags, " +
        " max(ts) over (partition by nodeid, time) as maxts, " +
        " ts, " +
        " startday " +
        " from " + Starter.STG_PREFIX+tableName +
        " where startday in ('" + Starter.filterPartitionToday + "') " +
        " ) a " +
        " where a.ts = a.maxts "
    )
  }

  def saveData(df: DataFrame, cassKeySpace: String) = {

    case class TrafficEventCounts(count: Long, aggregated_count: Long, trafficdetectioneventid: String)

    val StructTrafficEventCounts = StructType(
      StructField("count", LongType, true) ::
        StructField("aggregated_count", LongType, true) ::
        StructField("trafficdetectioneventid", StringType, true) ::
        Nil)

    // df.write.mode(SaveMode.Append).saveAsTable("traffic_status")
    df.createOrReplaceTempView("batchTrafficStatus")

    val dfEventCounts = JobSessions.spark.sql(
      " select count(*) as cnt, trafficdetectioneventid " +
        " from   batchTrafficStatus " +
        " group  by trafficdetectioneventid "
    )
    dfEventCounts.createOrReplaceTempView("batchEventCounts")

    val cass_table = JobSessions.sc.cassandraTable(cassKeySpace, "traffic_current_status")
      .select("count","aggregated_count","trafficdetectioneventid")

    import JobSessions.spark.implicits._

    val rdd = cass_table.map(r => Row(
      r.get[Option[Long]](0).getOrElse(0L),
      r.get[Option[Long]](1).getOrElse(0L),
      r.getString(2)
    ))

    val dff = JobSessions.spark.createDataFrame(rdd,StructTrafficEventCounts)

    dff.createOrReplaceTempView("trafficEventCounts")

    val dfUpd = JobSessions.spark.sql(
      " select " +
        "   tec.trafficdetectioneventid as trafficdetectioneventid, " +
        "   case when tec.aggregated_count = 0 then tec.count else aggregated_count end " +
        "   + " +
        "   case when bec.cnt is null then 0 else bec.cnt end " +
        "   as aggregated_count " +
        " from   trafficEventCounts tec " +
        " LEFT   OUTER JOIN batchEventCounts bec ON tec.trafficdetectioneventid = bec.trafficdetectioneventid "
    )

    dfUpd.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "traffic_current_status", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

  }

}


object ProcessAggregationParkingSpot {

  case class PartitionList(siteid: String, parkingtype: String)

  case class AggregationParkingSpot(
                                     siteid: String, parkingtype: String, starttime: Long, parkingspotid: String, groupid: String, zoneid: String,
                                     endtime: Long, occduration: Long, turnovers: Int, occpercent: Double, startdt: String, enddt: String,
                                     starthr: String, startday: String, ts: Long
                                   )

  val tableName = "aggregation_parking_spot"
  val rootLogger = Logger.getRootLogger

  def run(startDt: DateTime, untilDt: DateTime, cassKeySpace: String) = {

    import JobSessions.spark.implicits._

    CreateDWHTable

    JobSessions.spark.emptyDataset[AggregationParkingSpot].toDF().createOrReplaceTempView("allData")
    JobSessions.spark.emptyDataset[AggregationParkingSpot].toDF().createOrReplaceTempView("allEmpty")

    val from = (startDt.getMillis * 1000).toString
    val until = (untilDt.getMillis * 1000).toString
    var tmpCnt = 0L

    GetPartitions(from).collect().foreach(r=> {
      var siteid = r.getString(0)
      var parkingtype = r.getString(1)

      import scala.collection.convert.wrapAll._
      val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

      val stmt =
        " select siteid,parkingtype,starttime,parkingspotid,groupid,zoneid,endtime,occduration,turnovers,occpercent,startdt,enddt,starthr,startday,ts " +
          " from " + Starter.inp_cassKeyspace + "." + tableName +
          " where  siteid = '" + siteid + "' " +
          " and    parkingtype = '" + parkingtype + "' " +
          " and    starttime >= " + from + " " +
          " and    starttime <  " + until + " " +
          " allow  filtering "

      val cassData = cassSession.execute(stmt).all().toList

      val rdd = cassData.map(r => new AggregationParkingSpot(
        r.getString("siteid"),
        r.getString("parkingtype"),
        r.getLong("starttime"),
        r.getString("parkingspotid"),
        r.getString("groupid"),
        r.getString("zoneid"),
        r.getLong("endtime"),
        r.getLong("occduration"),
        r.getInt("turnovers"),
        r.getDouble("occpercent"),
        r.getString("startdt"),
        r.getString("enddt"),
        r.getString("starthr"),
        r.getString("startday"),
        r.getLong("ts")
      ))

      val df = rdd.toDF()
      tmpCnt += 1L

      if (tmpCnt > Starter.flushCount) {
        val dfTemp = JobSessions.spark.sql("select * from allData")
        InsertToSTG(dfTemp)

        val dfEmpty = JobSessions.spark.sql("select * from allEmpty")
        dfEmpty.createOrReplaceTempView("allData")

        tmpCnt = 0L
      }
      val newdf = df.union(JobSessions.spark.sql("select * from allData"))
      newdf.createOrReplaceTempView("allData")

      cassSession.close()
    })

    val df = JobSessions.spark.sql("select * from allData")
    InsertToSTG(df)

    Starter.CleanupStaging(tableName)  // manual
    Starter.CleanupWarehouse(tableName) // manual
    UpdateDWH // manual
    Starter.CleanupSave(tableName)
  }

  def CreateDWHTable = {

    val colList =
      " siteid string, " +
        " parkingtype string, " +
        " starttime bigint, " +
        " parkingspotid string," +
        " groupid string, " +
        " zoneid string, " +
        " endtime bigint, " +
        " occduration bigint, " +
        " turnovers int, " +
        " occpercent double, " +
        " startdt string, " +
        " enddt string, " +
        " starthr string, " +
        " ts bigint, " +
        " startdt_utc string, " +
        " starthr_utc string, " +
        " enddt_utc string, " +
        " startday_utc string "

    JobSessions.spark.sql(
      " create table if not exists " + Starter.STG_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )

    JobSessions.spark.sql(
      " create table if not exists " + Starter.DWH_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )
  }

  def InsertToSTG (df: DataFrame) ={
    df.createOrReplaceTempView("stgAggregationParkingSpot")
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.STG_PREFIX+tableName + " partition (startday) " +
        " select " +
        " a.siteid as siteid, " +
        " a.parkingtype as parkingtype, " +
        " a.starttime as starttime, " +
        " a.parkingspotid as parkingspotid, " +
        " a.groupid as groupid, " +
        " a.zoneid as zoneid, " +
        " a.endtime as endtime, " +
        " a.occduration as occduration, " +
        " a.turnovers as turnovers, " +
        " a.occpercent as occpercent, " +
        " from_utc_timestamp(a.startdt, s.time_zone) as startdt, " +
        " from_utc_timestamp(a.enddt, s.time_zone) as enddt, " +
        " substring(from_utc_timestamp(a.startdt, s.time_zone),1,13) as starthr, " +
        " unix_timestamp() as ts, " +
        " a.startdt as startdt_utc, " +
        " a.starthr as starthr_utc, " +
        " a.enddt as enddt_utc, " +
        " a.startday as startday_utc, " +
        " substring(from_utc_timestamp(a.startdt, s.time_zone),1,10) as startday " +
        " from stgAggregationParkingSpot a " +
        " JOIN " + Starter.DWH_PREFIX + "site s ON s.siteid = a.siteid "
    )
  }

  def UpdateDWH ={
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.DWH_PREFIX+tableName + " partition (startday) " +
        " select  siteid,parkingtype,starttime,parkingspotid,groupid,zoneid,endtime,occduration,turnovers," +
        "         occpercent,startdt,enddt,starthr,ts,startdt_utc,starthr_utc,enddt_utc,startday_utc,startday " +
        " from " +
        " ( " +
        " select " +
        " siteid, " +
        " parkingtype, " +
        " starttime, " +
        " parkingspotid, " +
        " groupid, " +
        " zoneid, " +
        " endtime, " +
        " occduration, " +
        " turnovers, " +
        " occpercent, " +
        " startdt, " +
        " enddt, " +
        " starthr, " +
        " max(ts) over (partition by parkingspotid, starttime) as maxts, " +
        " ts, " +
        " startdt_utc, " +
        " starthr_utc, " +
        " enddt_utc, " +
        " startday_utc, " +
        " startday " +
        " from " + Starter.STG_PREFIX+tableName +
        " where startday in ('" + Starter.filterPartitionYesterday + "', '" + Starter.filterPartitionToday + "') " +
        " ) a " +
        " where a.ts = a.maxts "
    )
  }

  def GetPartitions(from: String): DataFrame = {
    import JobSessions.spark.implicits._
    import scala.collection.convert.wrapAll._
    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

    val stmt =
      " select distinct siteid, parkingtype " +
        " from " + Starter.inp_cassKeyspace + "." + tableName +
        //      " where  starttime >= " + from + " " +
        " "
    //      " allow  filtering "

    val nodelist = cassSession.execute(stmt).all().toList

    val df = nodelist.map(r => new PartitionList(
      r.getString(0),
      r.getString(1)
    )).toDF().distinct()

    cassSession.close()
    return df
  }

}


object ProcessAggregationTrafficEvents {

  case class PartitionList(siteid: String, eventid: String, aggregation_type: String)

  case class AggregationTrafficEvents(
                                       siteid: String, nodeid: String, aggregation_type:String, starttime: Long,
                                       avgvelocity: Double, enddt: String, endtime: Long,
                                       eventcnt: Long, eventid: String, eventtype: String, eventname: String, medianvelocity: Double,
                                       objectclass: String, p85velocity: Double, avgdwell: Double, startday: String,
                                       startdt: String, starthr: String, ts: Long
                                     )

  val tableName = "aggregation_traffic_events"
  val rootLogger = Logger.getRootLogger

  def run(startDt: DateTime, untilDt: DateTime, cassKeySpace: String) = {

    import JobSessions.spark.implicits._

    CreateDWHTable

    JobSessions.spark.emptyDataset[AggregationTrafficEvents].toDF().createOrReplaceTempView("allData")
    JobSessions.spark.emptyDataset[AggregationTrafficEvents].toDF().createOrReplaceTempView("allEmpty")

    val from = (startDt.getMillis * 1000).toString
    val until = (untilDt.getMillis * 1000).toString
    var tmpCnt = 0L

    import scala.collection.convert.wrapAll._
    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

    val stmt =
      " select siteid,nodeid,aggregation_type,starttime,avgvelocity,enddt,endtime,eventcnt,eventid,type,name,medianvelocity,objectclass,p85velocity,avgdwell,startday,startdt,starthr,ts " +
        " from " + Starter.inp_cassKeyspace + "." + tableName +
        " where  starttime >= " + from + " " +
        " and    starttime <  " + until + " " +
        " allow  filtering "

    val cassData = cassSession.execute(stmt).all().toList

    val rdd = cassData.map(r => new AggregationTrafficEvents(
      r.getString("siteid"),
      r.getString("nodeid"),
      r.getString("aggregation_type"),
      r.getLong("starttime"),
      r.getDouble("avgvelocity"),
      r.getString("enddt"),
      r.getLong("endtime"),
      r.getLong("eventcnt"),
      r.getString("eventid"),
      r.getString("type"),
      r.getString("name"),
      r.getDouble("medianvelocity"),
      r.getString("objectclass"),
      r.getDouble("p85velocity"),
      r.getDouble("avgdwell"),
      r.getString("startday"),
      r.getString("startdt"),
      r.getString("starthr"),
      r.getLong("ts")
    ))

    val dfi = rdd.toDF()
    tmpCnt += 1L

    if (tmpCnt > Starter.flushCount) {
      val dfTemp = JobSessions.spark.sql("select * from allData")
      InsertToSTG(dfTemp)

      val dfEmpty = JobSessions.spark.sql("select * from allEmpty")
      dfEmpty.createOrReplaceTempView("allData")

      tmpCnt = 0L
    }

    val newdf = dfi.union(JobSessions.spark.sql("select * from allData"))
    newdf.createOrReplaceTempView("allData")

    cassSession.close()
    //    })

    val df = JobSessions.spark.sql("select * from allData")
    InsertToSTG(df)

    Starter.CleanupStaging(tableName)
    Starter.CleanupWarehouse(tableName)
    UpdateDWH
    Starter.CleanupSave(tableName)
  }

  def CreateDWHTable = {

    val colList =
      " siteid string, " +
        " nodeid string, " +
        " aggregation_type string, " +
        " starttime bigint, " +
        " avgvelocity double," +
        " enddt string, " +
        " endtime bigint, " +
        " eventcnt bigint, " +
        " eventid string, " +
        " eventtype string, " +
        " eventname string, " +
        " medianvelocity double, " +
        " objectclass string, " +
        " p85velocity double, " +
        " avgdwell double, " +
        " startdt string, " +
        " starthr string, " +
        " ts bigint, " +
        " startdt_utc string, " +
        " starthr_utc string, " +
        " enddt_utc string, " +
        " startday_utc string "

    JobSessions.spark.sql(
      " create table if not exists " + Starter.STG_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )

    JobSessions.spark.sql(
      " create table if not exists " + Starter.DWH_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )
  }

  def InsertToSTG (df: DataFrame) ={
    df.createOrReplaceTempView("stgAggregationTrafficEvents")
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.STG_PREFIX+tableName + " partition (startday) " +
        " select " +
        " a.siteid as siteid, " +
        " a.nodeid as nodeid, " +
        " a.aggregation_type as aggregation_type, " +
        " a.starttime as starttime, " +
        " a.avgvelocity as avgvelocity," +
        " from_utc_timestamp(a.enddt, s.time_zone) as enddt, " +
        " a.endtime as endtime, " +
        " a.eventcnt as eventcnt, " +
        " a.eventid as eventid, " +
        " a.eventtype as eventtype, " +
        " a.eventname as eventname, " +
        " a.medianvelocity as medianvelocity, " +
        " a.objectclass as objectclass, " +
        " a.p85velocity as p85velocity, " +
        " a.avgdwell as avgdwell, " +
        " from_utc_timestamp(a.startdt, s.time_zone) as startdt, " +
        " substring(from_utc_timestamp(a.startdt, s.time_zone),1,13) as starthr, " +
        " unix_timestamp() as ts, " +
        " a.startdt as startdt_utc, " +
        " a.starthr as starthr_utc, " +
        " a.enddt as enddt_utc, " +
        " a.startday as startday_utc, " +
        " substring(from_utc_timestamp(a.startdt, s.time_zone),1,10) as startday " +
        " from stgAggregationTrafficEvents a " +
        " JOIN " + Starter.DWH_PREFIX + "site s ON s.siteid = a.siteid "
    )
  }

  def UpdateDWH ={
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.DWH_PREFIX+tableName + " partition (startday) " +
        " select  siteid,nodeid,aggregation_type,starttime,avgvelocity,enddt,endtime,eventcnt,eventid,eventtype,eventname," +
        "         medianvelocity,objectclass,p85velocity,avgdwell,startdt,starthr,ts,startdt_utc,starthr_utc,enddt_utc,startday_utc,startday " +
        " from " +
        " ( " +
        " select " +
        " siteid, " +
        " nodeid, " +
        " aggregation_type, " +
        " starttime, " +
        " case avgvelocity when 0.0 then null else avgvelocity end as avgvelocity," +
        " enddt, " +
        " endtime, " +
        " eventcnt, " +
        " eventid, " +
        " eventtype, " +
        " eventname, " +
        " case medianvelocity when 0.0 then null else medianvelocity end as medianvelocity, " +
        " objectclass, " +
        " case p85velocity when 0.0 then null else p85velocity end as p85velocity, " +
        " case avgdwell when 0.0 then null else avgdwell end as avgdwell, " +
        " startdt, " +
        " starthr, " +
        " max(ts) over (partition by eventid, aggregation_type, starttime) as maxts, " +
        " ts, " +
        " startdt_utc, " +
        " starthr_utc, " +
        " enddt_utc, " +
        " startday_utc, " +
        " startday " +
        " from " + Starter.STG_PREFIX+tableName +
        " where startday in ('" + Starter.filterPartitionYesterday + "', '" + Starter.filterPartitionToday + "') " +
        " ) a " +
        " where a.ts = a.maxts "
    )
  }

  def GetPartitions(from: String): DataFrame = {
    import JobSessions.spark.implicits._
    import scala.collection.convert.wrapAll._
    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

    val stmt =
      " select siteid, eventid, aggregation_type " +
        " from " + Starter.inp_cassKeyspace + "." + tableName +
        " where  starttime >= " + from + " " +
        " allow  filtering "

    val nodelist = cassSession.execute(stmt).all().toList

    val df = nodelist.map(r => new PartitionList(
      r.getString(0),
      r.getString(1),
      r.getString(2)
    )).toDF().distinct()

    cassSession.close()
    return df
  }

}


object ProcessEnergySavings {

  case class PartitionList(siteid: String, nodeid: String, aggregation_type: String)

  case class AggregationEnergySavings(
                                       siteid: String, nodeid: String, aggregation_type:String, starttime: Long,
                                       actual_energy_consumption: Double, enddt: String, endtime: Long,
                                       led_energy_consumption: Double, legacy_energy_consumption: Double,
                                       savings_legacy_vs_actual: Double, savings_legacy_vs_led: Double,
                                       startday: String, startdt: String, starthr: String, ts: Long
                                     )

  val tableName = "aggregation_energy_savings_node"
  val rootLogger = Logger.getRootLogger

  def run(startDt: DateTime, untilDt: DateTime, cassKeySpace: String) = {

    import JobSessions.spark.implicits._

    CreateDWHTable

    JobSessions.spark.emptyDataset[AggregationEnergySavings].toDF().createOrReplaceTempView("allData")
    JobSessions.spark.emptyDataset[AggregationEnergySavings].toDF().createOrReplaceTempView("allEmpty")

    val from = (startDt.getMillis * 1000).toString
    val until = (untilDt.getMillis * 1000).toString
    var tmpCnt = 0L

    //    GetPartitions(from).collect().foreach(r=> {
    //      var siteid = r.getString(0)
    //      var nodeid = r.getString(1)
    //      var aggregation_type = r.getString(2)

    import scala.collection.convert.wrapAll._
    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

    val stmt =
      " select siteid,nodeid,aggregation_type,starttime,actual_energy_consumption,enddt,endtime,led_energy_consumption," +
        "        legacy_energy_consumption,savings_legacy_vs_actual,savings_legacy_vs_led,startday,startdt,starthr,ts " +
        " from " + Starter.inp_cassKeyspace + "." + tableName +
        //        " where  siteid = '" + siteid + "' " +
        //        " and    nodeid = '" + nodeid + "' " +
        //        " and    aggregation_type = '" + aggregation_type + "' " +
        " where    starttime >= " + from + " " +
        " and    starttime <  " + until + " " +
        " allow  filtering "

    val cassData = cassSession.execute(stmt).all().toList

    val rdd = cassData.map(r => new AggregationEnergySavings(
      r.getString("siteid"),
      r.getString("nodeid"),
      r.getString("aggregation_type"),
      r.getLong("starttime"),
      r.getDouble("actual_energy_consumption"),
      r.getString("enddt"),
      r.getLong("endtime"),
      r.getDouble("led_energy_consumption"),
      r.getDouble("legacy_energy_consumption"),
      r.getDouble("savings_legacy_vs_actual"),
      r.getDouble("savings_legacy_vs_led"),
      r.getString("startday"),
      r.getString("startdt"),
      r.getString("starthr"),
      r.getLong("ts")
    ))

    val df = rdd.toDF()
    tmpCnt += 1L

    if (tmpCnt > Starter.flushCount) {
      val dfTemp = JobSessions.spark.sql("select * from allData")
      InsertToSTG(dfTemp)

      val dfEmpty = JobSessions.spark.sql("select * from allEmpty")
      dfEmpty.createOrReplaceTempView("allData")

      tmpCnt = 0L
    }
    val newdf = df.union(JobSessions.spark.sql("select * from allData"))
    newdf.createOrReplaceTempView("allData")

    cassSession.close()
    //    })

    val dfi = JobSessions.spark.sql("select * from allData")
    InsertToSTG(dfi)

    Starter.CleanupStaging(tableName) // manual
    Starter.CleanupWarehouse(tableName) // manual
    UpdateDWH // manual
    Starter.CleanupSave(tableName)
  }

  def CreateDWHTable = {

    // ilker - add startdt_utc, starthr_utc, enddt_utc, startday_utc, manually alter STG and dwh tables
    val colList =
      " siteid string, " +
        " nodeid string, " +
        " aggregation_type string, " +
        " starttime bigint, " +
        " actual_energy_consumption double," +
        " enddt string, " +
        " endtime bigint, " +
        " led_energy_consumption double, " +
        " legacy_energy_consumption double, " +
        " savings_legacy_vs_actual double, " +
        " savings_legacy_vs_led double, " +
        " startdt string, " +
        " starthr string, " +
        " ts bigint, " +
        " startdt_utc string, " +
        " starthr_utc string, " +
        " enddt_utc string, " +
        " startday_utc string "

    JobSessions.spark.sql(
      " create table if not exists " + Starter.STG_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        //        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
        " STORED AS ORC "
    )

    JobSessions.spark.sql(
      " create table if not exists " + Starter.DWH_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        //        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
        " STORED AS ORC "
    )
  }

  def InsertToSTG (df: DataFrame) ={
    df.createOrReplaceTempView("stgAggregationEnergySavings")
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      // ilker - join dwh_site (siteid, time_zone)
      // ilker - add startdt_utc, enddt_utc, startday_utc before startday and also add timezone conversion for all 4 original cols
      // ilker substring for starthr and startday substring(x,1,13)
      // ilker - use alias for col names
      " insert into " + Starter.STG_PREFIX+tableName + " partition (startday) " +
        " select " +
        " a.siteid as siteid, " +
        " a.nodeid as nodeid, " +
        " a.aggregation_type as aggregation_type, " +
        " a.starttime as starttime, " +
        " a.actual_energy_consumption as actual_energy_consumption," +
        " from_utc_timestamp(a.enddt, s.time_zone) as enddt, " +
        " a.endtime as endtime, " +
        " a.led_energy_consumption as led_energy_consumption, " +
        " a.legacy_energy_consumption as legacy_energy_consumption, " +
        " a.savings_legacy_vs_actual as savings_legacy_vs_actual, " +
        " a.savings_legacy_vs_led as savings_legacy_vs_led, " +
        " from_utc_timestamp(a.startdt, s.time_zone) as startdt, " +
        " substring(from_utc_timestamp(a.startdt, s.time_zone),1,13) as starthr, " +
        " unix_timestamp() as ts, " +
        " a.startdt as startdt_utc, " +
        " a.starthr as starthr_utc, " +
        " a.enddt as enddt_utc, " +
        " a.startday as startday_utc, " +
        " substring(from_utc_timestamp(a.startdt, s.time_zone),1,10) as startday " +
        " from stgAggregationEnergySavings a " +
        " JOIN " + Starter.DWH_PREFIX + "site s ON s.siteid = a.siteid "
    )
  }

  def UpdateDWH ={
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      // ilker - add startdt_utc, starthr_utc, enddt_utc, startday_utc before startday and also in subquery
      " insert into " + Starter.DWH_PREFIX+tableName + " partition (startday) " +
        " select  siteid,nodeid,aggregation_type,starttime,actual_energy_consumption,enddt,endtime," +
        "         led_energy_consumption,legacy_energy_consumption,savings_legacy_vs_actual,savings_legacy_vs_led," +
        "         startdt,starthr,ts,startdt_utc,starthr_utc,enddt_utc,startday_utc,startday " +
        " from " +
        " ( " +
        " select " +
        " siteid, " +
        " nodeid, " +
        " aggregation_type, " +
        " starttime, " +
        " actual_energy_consumption," +
        " enddt, " +
        " endtime, " +
        " led_energy_consumption, " +
        " legacy_energy_consumption, " +
        " savings_legacy_vs_actual, " +
        " savings_legacy_vs_led, " +
        " startdt, " +
        " starthr, " +
        " max(ts) over (partition by nodeid, aggregation_type, starttime) as maxts, " +
        " ts, " +
        " startdt_utc, " +
        " starthr_utc, " +
        " enddt_utc, " +
        " startday_utc, " +
        " startday " +
        " from " + Starter.STG_PREFIX+tableName +
        " where startday in ('" + Starter.filterPartitionYesterday + "', '" + Starter.filterPartitionToday + "') " +
        " ) a " +
        " where a.ts = a.maxts "
    )
  }

  def GetPartitions(from: String): DataFrame = {
    import JobSessions.spark.implicits._
    import scala.collection.convert.wrapAll._
    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

    val stmt =
      " select siteid, nodeid, aggregation_type " +
        " from " + Starter.inp_cassKeyspace + "." + tableName +
        " where  starttime >= " + from + " " +
        " allow  filtering "

    val nodelist = cassSession.execute(stmt).all().toList

    val df = nodelist.map(r => new PartitionList(
      r.getString(0),
      r.getString(1),
      r.getString(2)
    )).toDF().distinct()

    cassSession.close()
    return df
  }

}


object ProcessParkingZoneStatus {

  var listCapacity = ListBuffer[Row]()

  case class ParkingZoneStatus(
                                siteid: String, orgid: String, parkinggroupid:String,
                                parkingzoneid: String, parkingtype: String, capacity: Int, config:String
                              )

  case class ParkingSpotZone(parkingspotid: String, parkingzoneid: String, demarcated: Boolean, active: Boolean )

  val tableName = "parking_zone_status"
  val DWHtName = Starter.DWH_PREFIX + "parking_zone_status"
  val rootLogger = Logger.getRootLogger

  def run(cassKeySpace: String) = {

    import JobSessions.spark.implicits._

    CreateDWHTable

    JobSessions.spark.emptyDataset[ParkingZoneStatus].toDF().createOrReplaceTempView("allData")

    import scala.collection.convert.wrapAll._
    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

    val cass_table = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "parking_zone_status")
      .select("siteid", "orgid", "parkinggroupid", "parkingzoneid", "type", "config")
    val rdd = cass_table.map(r => new ParkingZoneStatus(
      r.getString("siteid"),
      r.getString("orgid"),
      r.getString("parkinggroupid"),
      r.getString("parkingzoneid"),
      r.getString("type"),
      0,
      r.get[Option[String]]("config").getOrElse("na")
    ))
    val DFparkingZones = rdd.toDF()


    val cass_table_cur = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "parking_spot_current")
      .select("parkingspotid","parkingzoneid","demarcated","active")
    val rddz = cass_table_cur.map(r => new ParkingSpotZone(
      r.getString("parkingspotid"),
      //r.getString("parkingzoneid"),
      r.get[Option[String]]("parkingzoneid").getOrElse("na"),
      //r.getBoolean("demarcated"),
      r.get[Option[Boolean]]("demarcated").getOrElse(false),
      //r.getBoolean("active")
      r.get[Option[Boolean]]("active").getOrElse(false)
    ))
    val DFparkingSpotZones = rddz.toDF()
    DFparkingSpotZones.createOrReplaceTempView("DFparkingSpotZones")

    val dfZoneCapacity = GetZoneCapacity(DFparkingZones, DFparkingSpotZones)
    dfZoneCapacity.cache()
    dfZoneCapacity.createOrReplaceTempView("dfParkingZoneStatus")

    //val df = rdd.toDF()
    InsertToTable(dfZoneCapacity)
    cassSession.close()
  }

  def CreateDWHTable = {

    val colList =
      " siteid string, " +
        " orgid string, " +
        " groupid string, " +
        " zoneid string, " +
        " parkingtype string, " +
        " capacity int," +
        " config string," +
        " ts bigint "

    JobSessions.spark.sql(
      " drop table if exists " + DWHtName
    )

    JobSessions.spark.sql(
      " create table if not exists " + DWHtName + " ( " + colList + " ) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )
  }

  def InsertToTable (df: DataFrame) ={
    df.createOrReplaceTempView("dfParkingZoneStatus")
    JobSessions.spark.sql(
      " insert into " + DWHtName +
        " select " +
        " siteid, " +
        " orgid, " +
        " parkinggroupid as groupid, " +
        " parkingzoneid as zoneid, " +
        " parkingtype, " +
        " case when capacity = 0 then null else capacity end as capacity, " +
        " config, " +
        " unix_timestamp() as ts " +
        " from dfParkingZoneStatus "
    )
  }

  def GetZoneCapacity (DFparkingZones:DataFrame, DFspotZones:DataFrame): DataFrame = {

    val StructCapacity = StructType(
      StructField("parkingzoneid", StringType, false) ::
        StructField("capacity", IntegerType, false) ::
        Nil)

    DFparkingZones.createOrReplaceTempView("DFparkingZones")
    DFspotZones.createOrReplaceTempView("DFspotZones")

    JobSessions.spark.sql(
      "select parkingzoneid, count(distinct parkingspotid) as capacity from DFspotZones where demarcated = true and active = true group by parkingzoneid "
    ).createOrReplaceTempView("capacity_demarcated")

    val dfDemarcatedCapacity = JobSessions.spark.sql(
      " select z.siteid as siteid, z.orgid as orgid, z.parkinggroupid as parkinggroupid, z.parkingzoneid as parkingzoneid, z.parkingtype, " +
        "        c.capacity as capacity, z.config as config " +
        " from   DFparkingZones z " +
        " LEFT   OUTER JOIN capacity_demarcated c ON c.parkingzoneid = z.parkingzoneid " +
        " where  z.parkingtype = 'Demarcated' "
    )

    JobSessions.spark.sql(" select parkingzoneid, config from DFparkingZones where parkingtype = 'NonDemarcated' and config != 'na' ")
      .collect().foreach(r=>{
      // do the magic here
      var capacity = 0
      var lat1 = ""
      var lat2 = ""
      var lon1 = ""
      var lon2 = ""
      try {
        val x = parse(r.getString(1))
        val roi = (x \ "roi")
        var wbb = (roi \ "world_bounding_box")(0)
        //println(compact(render(( wbb ))).replaceAll("\"", "").toLowerCase())

        try {
          lat1 = compact(render(( wbb(0) \ "latitude" ))).replaceAll("\"", "").toLowerCase()
          lat2 = compact(render(( wbb(2) \ "latitude" ))).replaceAll("\"", "").toLowerCase()
          lon1 = compact(render(( wbb(0) \ "longitude" ))).replaceAll("\"", "").toLowerCase()
          lon2 = compact(render(( wbb(2) \ "longitude" ))).replaceAll("\"", "").toLowerCase()

          /*
          wbb = (roi \ "world_bounding_box")(1)
          lat1 = compact(render(( wbb(0) \ "latitude" ))).replaceAll("\"", "").toLowerCase()
          lat2 = compact(render(( wbb(2) \ "latitude" ))).replaceAll("\"", "").toLowerCase()
          lon1 = compact(render(( wbb(0) \ "longitude" ))).replaceAll("\"", "").toLowerCase()
          lon2 = compact(render(( wbb(2) \ "longitude" ))).replaceAll("\"", "").toLowerCase()
          */

        }
        catch {
          case ex: Exception => {
            lat1 = ""
            lat2 = ""
            lon1 = ""
            lon2 = ""
          }
        }

        var d = 0.0
        if (lat1.isEmpty || lat2.isEmpty || lon1.isEmpty || lon2.isEmpty) {
          capacity = 0
        } else {
          val f1 = Math.toRadians(lat1.toDouble)
          val f2 = Math.toRadians(lat2.toDouble)
          val dl = Math.toRadians(lon2.toDouble - lon1.toDouble)
          val R = 6371e3
          val x = dl * Math.cos((f1 + f2) / 2.0)
          val y = f2 - f1;
          val d = Math.sqrt(x * x + y * y) * R
          //d.toFixed(2)
          capacity = Math.round(d / 4.9).toInt
        }
      }
      catch {
        case ex: Exception => {
          capacity = 0
        }
      }

      listCapacity += Row(r.getString(0), capacity)
    })

    val list = listCapacity.toList
    val rdd = JobSessions.sc.makeRDD(list)
    JobSessions.spark.createDataFrame(rdd, StructCapacity).createOrReplaceTempView("capacity_nondemarcated")

    val dfNonDemarcatedCapacity = JobSessions.spark.sql(
      " select z.siteid as siteid, z.orgid as orgid, z.parkinggroupid as parkinggroupid, z.parkingzoneid as parkingzoneid, z.parkingtype, " +
        "        c.capacity as capacity, z.config as config " +
        " from   dfParkingZones z " +
        " LEFT   OUTER JOIN capacity_nondemarcated c ON c.parkingzoneid = z.parkingzoneid " +
        " where  z.parkingtype = 'NonDemarcated' "
    )

    return dfDemarcatedCapacity.union(dfNonDemarcatedCapacity)
  }

}


object ProcessParkingSpotCurrent {

  case class ParkingSpotCurrent  (
                                   parkingspotid:String, active:Boolean, activesince:Long, address:String,
                                   altitude:Int, channel:Int, demarcated:Boolean, description:String,
                                   lat1:Float, lat2:Float, lat3:Float, lat4:Float,
                                   lng1:Float, lng2:Float, lng3:Float, lng4:Float,
                                   nodeid:String, objectid:String, occupancy:Boolean, orgid:String,
                                   parkinggroupid:String, parkingzoneid:String, since:Long, siteid:String,tags:String,
                                   x1:Float, x2:Float, x3:Float, x4:Float, y1:Float, y2:Float, y3:Float, y4:Float
                                 )

  val tableName = "parking_spot_current"
  val DWHtName = Starter.DWH_PREFIX + "parking_spot"
  val rootLogger = Logger.getRootLogger

  def run(cassKeySpace: String) = {

    import JobSessions.spark.implicits._

    CreateDWHTable

    JobSessions.spark.emptyDataset[ParkingSpotCurrent].toDF().createOrReplaceTempView("allData")

    import scala.collection.convert.wrapAll._
    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

    val stmt =
      " select " +
        " parkingspotid,active,activesince,address, " +
        " altitude,channel,demarcated,description, " +
        " lat1,lat2,lat3,lat4, " +
        " lng1,lng2,lng3,lng4, " +
        " nodeid,objectid,occupancy,orgid, " +
        " parkinggroupid,parkingzoneid,since,siteid,tags, " +
        " x1,x2,x3,x4, " +
        " y1,y2,y3,y4 " +
        " from " + Starter.inp_cassKeyspace + "." + tableName

    val cassData = cassSession.execute(stmt).all().toList

    val rdd = cassData.map(r => new ParkingSpotCurrent(
      r.getString("parkingspotid"),
      r.getBool("active"),
      r.getLong("activesince"),
      r.getString("address"),
      r.getInt("altitude"),
      r.getInt("channel"),
      r.getBool("demarcated"),
      r.getString("description"),
      r.getFloat("lat1"),
      r.getFloat("lat2"),
      r.getFloat("lat3"),
      r.getFloat("lat4"),
      r.getFloat("lng1"),
      r.getFloat("lng2"),
      r.getFloat("lng3"),
      r.getFloat("lng4"),
      r.getString("nodeid"),
      r.getString("objectid"),
      r.getBool("occupancy"),
      r.getString("orgid"),
      r.getString("parkinggroupid"),
      r.getString("parkingzoneid"),
      r.getLong("since"),
      r.getString("siteid"),
      r.getString("tags"),
      r.getFloat("x1"),
      r.getFloat("x2"),
      r.getFloat("x3"),
      r.getFloat("x4"),
      r.getFloat("y1"),
      r.getFloat("y2"),
      r.getFloat("y3"),
      r.getFloat("y4")
    ))

    val df = rdd.toDF()
    InsertToTable(df)
    cassSession.close()
  }

  def CreateDWHTable = {

    val colList =
      " parkingspotid string, " +
        " active boolean, " +
        " activesince bigint, " +
        " address string, " +
        " altitude int, " +
        " channel int, " +
        " demarcated boolean, " +
        " description string, " +
        " lat1 float, " +
        " lat2 float, " +
        " lat3 float, " +
        " lat4 float, " +
        " lng1 float, " +
        " lng2 float, " +
        " lng3 float, " +
        " lng4 float, " +
        " nodeid string, " +
        " objectid string, " +
        " occupancy boolean, " +
        " orgid string, " +
        " parkinggroupid string, " +
        " parkingzoneid string, " +
        " since bigint, " +
        " siteid string, " +
        " tags string, " +
        " x1 float, " +
        " x2 float, " +
        " x3 float, " +
        " x4 float, " +
        " y1 float, " +
        " y2 float, " +
        " y3 float, " +
        " y4 float, " +
        " ts bigint "

    JobSessions.spark.sql(
      " drop table if exists " + DWHtName
    )

    JobSessions.spark.sql(
      " create table if not exists " + DWHtName + " ( " + colList + " ) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )
  }

  def InsertToTable (df: DataFrame) ={
    df.createOrReplaceTempView("dfParkingSpotCurrent")
    JobSessions.spark.sql(
      " insert into " + DWHtName +
        " select " +
        " parkingspotid, " +
        " active, " +
        " activesince, " +
        " address, " +
        " altitude, " +
        " channel, " +
        " demarcated, " +
        " description, " +
        " lat1, " +
        " lat2, " +
        " lat3, " +
        " lat4, " +
        " lng1, " +
        " lng2, " +
        " lng3, " +
        " lng4, " +
        " nodeid, " +
        " objectid, " +
        " occupancy, " +
        " orgid, " +
        " parkinggroupid, " +
        " parkingzoneid, " +
        " since, " +
        " siteid, " +
        " tags, " +
        " x1, " +
        " x2, " +
        " x3, " +
        " x4, " +
        " y1, " +
        " y2, " +
        " y3, " +
        " y4, " +
        " unix_timestamp() as ts " +
        " from dfParkingSpotCurrent "
    )
  }

}


object ProcessDeviceSensorSamples {

  case class PartitionList(nodeid:String)

  case class DeviceSensorSamples(
                                  nodeid: String, sensor:String, time:Long, value:Double
                                )

  val sensorList = " ('mPF','jt','aP','ai','rlya','podme','i','P','aPF','WDT','l-i','bc','bR','pnd','jtx','mip'," +
    "  'vp','aw','PF','lIR-i','RF','rf','podm','jtm','bRA','mt','lt','pc','jty','mw','t','aip'," +
    "  'lIR','pdc','mi','pdt','sn','p','jtz','bRL','T','mP','w','v','rlym','l','ppr') "

  val tableName = "device_sensor_samples"
  val rootLogger = Logger.getRootLogger

  def run(startDt: DateTime, untilDt: DateTime, cassKeySpace: String) = {

    import JobSessions.spark.implicits._

    CreateDWHTable

    JobSessions.spark.emptyDataset[DeviceSensorSamples].toDF().createOrReplaceTempView("allData")
    JobSessions.spark.emptyDataset[DeviceSensorSamples].toDF().createOrReplaceTempView("allEmpty")

    val from = (startDt.getMillis * 1000).toString
    val until = (untilDt.getMillis * 1000).toString
    var tmpCnt = 0L

    val DFnodes = GetPartitions(from)
    //val nodeCnt = DFnodes.count()
    val DFnodeGroup = Starter.pivotGroup(DFnodes, 100, "String") // 100 prod , 400 scale now 100


    DFnodeGroup.collect().foreach(r=> {
      var nodeidGrp = r.getString(0)

      import scala.collection.convert.wrapAll._
      val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

      val stmt =
        " select nodeid, sensor, time, value " +
          " from " + Starter.inp_cassKeyspace + "." + tableName +
          " where  nodeid in (" + nodeidGrp + ") " +
          " and    sensor in " + sensorList + " " +
          " and    time >= " + from +
          " and    time <  " + until +
          " limit " + Starter.rowLimit.toString + " "
          //" allow  filtering "

      val cassData = cassSession.execute(stmt).all().toList

      val rdd = cassData.map(r => new DeviceSensorSamples(
        r.getString("nodeid"),
        r.getString("sensor"),
        r.getLong("time"),
        r.getDouble("value")
      ))

      val df = rdd.toDF()
      tmpCnt += 1L

      if (tmpCnt > Starter.flushCount) {  // Starter.flushCount
      val dfTemp = JobSessions.spark.sql("select * from allData")
        InsertToSTG(dfTemp)

        val dfEmpty = JobSessions.spark.sql("select * from allEmpty")
        dfEmpty.createOrReplaceTempView("allData")

        tmpCnt = 0L
      }
      val newdf = df.union(JobSessions.spark.sql("select * from allData"))
      newdf.createOrReplaceTempView("allData")

      cassSession.close()
    })

    val df = JobSessions.spark.sql("select * from allData")
    InsertToSTG(df)

    rootLogger.info("hr = "+ DateTime.now(DateTimeZone.UTC).toString("HH"))

    if (DateTime.now(DateTimeZone.UTC).toString("HH") == "01") {
      Starter.CleanupStaging(tableName)
      Starter.CleanupWarehouse(tableName)
      UpdateDWH
      Starter.CleanupSave(tableName)
    }
    else {
      Starter.CleanupStaging(tableName)
      Starter.CleanupWarehouseUTC(tableName)
      UpdateDWHUTC
      Starter.CleanupSave(tableName)
    }

  }

  def CreateDWHTable = {

    val colList =
      " nodeid string, " +
        " sensor string, " +
        " time bigint, " +
        " value double, " +
        " ts bigint "

    JobSessions.spark.sql(
      " create table if not exists " + Starter.STG_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )

    JobSessions.spark.sql(
      " create table if not exists " + Starter.DWH_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )
  }

  def InsertToSTG (df: DataFrame) ={
    df.createOrReplaceTempView("stgDeviceSensorSamples")
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.STG_PREFIX+tableName + " partition (startday) " +
        " select " +
        " nodeid, " +
        " sensor, " +
        " time, " +
        " value, " +
        " unix_timestamp() as ts, " +
        " substr(from_unixtime(cast(time/1000000 as bigint)), 1, 10) as startday " +
        " from stgDeviceSensorSamples "
    )
  }

  def UpdateDWH ={
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.DWH_PREFIX+tableName + " partition (startday) " +
        " select  nodeid, sensor, time, value, ts, startday " +
        " from " +
        " ( " +
        " select " +
        " nodeid, " +
        " sensor, " +
        " time, " +
        " value, " +
        " max(ts) over (partition by nodeid, sensor, time) as maxts, " +
        " ts, " +
        " startday " +
        " from " + Starter.STG_PREFIX+tableName +
        " where startday in ('" + Starter.filterPartitionYesterday + "', '" + Starter.filterPartitionToday + "') " +
        " ) a " +
        " where a.ts = a.maxts "
    )
  }

  def UpdateDWHUTC ={
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.DWH_PREFIX+tableName + " partition (startday) " +
        " select  nodeid, sensor, time, value, ts, startday " +
        " from " +
        " ( " +
        " select " +
        " nodeid, " +
        " sensor, " +
        " time, " +
        " value, " +
        " max(ts) over (partition by nodeid, sensor, time) as maxts, " +
        " ts, " +
        " startday " +
        " from " + Starter.STG_PREFIX+tableName +
        " where startday in ('" + Starter.filterPartitionToday + "') " +
        " ) a " +
        " where a.ts = a.maxts "
    )
  }

  def GetPartitions(from: String): DataFrame = {

    import JobSessions.spark.implicits._
    import scala.collection.convert.wrapAll._

    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()
    val tableName = "aggregation_filters"

    val stmt =
      " select obj from " + Starter.inp_cassKeyspace + "." + tableName +
        " where  type = 'connection_nodes' "

    val nodelist = cassSession.execute(stmt).all().toList

    val df = nodelist.map(r => new PartitionList(
      r.getString(0)
    )).toDF().distinct()

    cassSession.close()
    return df
  }

  /*
  def GetPartitions(from: String): DataFrame = {
    import JobSessions.spark.implicits._
    import scala.collection.convert.wrapAll._
    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()
    val stmt =
      " select nodeid " +
      " from " + Starter.inp_cassKeyspace + "." + tableName +
      " where sensor in ('T') " +
      " and   time >= " + from +
      " and   time <  " + (from.toLong+(1000000*60*60)).toString +
      " allow filtering "
    val nodelist = cassSession.execute(stmt).all().toList
    val df = nodelist.map(r => new PartitionList(
      r.getString(0)
    )).toDF().distinct()
    /*
    val stmt =
      " select distinct nodeid from " + Starter.inp_cassKeyspace + ".node_status"
    val nodelist = cassSession.execute(stmt).all().toList
    val df = nodelist.map(r => new PartitionList(
      r.getString(0)
    )).toDF()
    */
    cassSession.close()
    return df
  }
  */

}


object ProcessDeviceAlarms {

  case class PartitionList(nodeid:String)

  case class DeviceAlarms(
                           nodeid: String, alarmtype:String, alarmdate:Long, dateuuid:String, message:String, severitycode:String, startday:String
                         )

  val alarmList =
      " ('Assert', 'Clear', 'Disconnect', 'HWFAIL_GPS', 'HWFail_EEPROM', 'HWFail_ISL29023', 'HWFail_MMA8451', " +
      " 'HWFail_NIGHTHAWK', 'HWFail_PodBus', 'HWFail_RTC', 'HWFail_SE95', 'HWFail_STUCK_RELAY', 'HWFail_ZMotion', 'HardFault', " +
      " 'Minor', 'PMACFail', 'SWUpdateFail_SENSORPOD', 'StrangeReboot', 'X509ClientFail', 'X509ServerFail', " +
      " 'CommFail', 'SimFail', 'USPFail', 'PMACFail', 'DriverFail', 'FarmUSPFail', 'HWFail_UrbanUSP', " +
      " 'SensorFail', 'HWFail_generic', 'HWFail_HIH6131', 'HWFail_TSC3414', 'SWUpdateFail_SensorPod', " +
      " 'HWFail_StuckRelay', 'HWFail_SiHawk', 'HWFail_GPS') "

  val tableName = "device_alarms"
  val rootLogger = Logger.getRootLogger

  def run(startDt: DateTime, untilDt: DateTime, cassKeySpace: String) = {

    import JobSessions.spark.implicits._

    CreateDWHTable

    JobSessions.spark.emptyDataset[DeviceAlarms].toDF().createOrReplaceTempView("allData")
    JobSessions.spark.emptyDataset[DeviceAlarms].toDF().createOrReplaceTempView("allEmpty")

    val from = (startDt.getMillis * 1000).toString
    val until = (untilDt.getMillis * 1000).toString
    var tmpCnt = 0L

    val DFnodes = GetPartitions(from)
    //val nodeCnt = DFnodes.count()
    val DFnodeGroup = Starter.pivotGroup(DFnodes, 100, "String")

    DFnodeGroup.collect().foreach(r=> {
      var nodeidGrp = r.getString(0)

      import scala.collection.convert.wrapAll._
      val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

      val stmt =
        " select nodeid, alarmtype, unixTimestampOf(date) as alarmdate, date as dateuuid, message, severitycode " +
          " from " + Starter.inp_cassKeyspace + "." + tableName +
          " where  nodeid in (" + nodeidGrp + ") " +
          " and    alarmtype in " + alarmList + " " +
          " and    date >= maxTimeuuid('" + Starter.getStringFromMicrosecs(from.toLong) + "') " +
          " and    date <  maxTimeuuid('" + Starter.getStringFromMicrosecs(until.toLong) + "') " +
          " limit " + Starter.rowLimit.toString +
          " allow  filtering "

      val cassData = cassSession.execute(stmt).all().toList

      val rdd = cassData.map(r => new DeviceAlarms(
        r.getString("nodeid"),
        r.getString("alarmtype"),
        r.getLong("alarmdate"),
        r.getUUID("dateuuid").toString,
        //r.getString("dateuuid"),
        r.getString("message"),
        r.getString("severitycode"),
        (new DateTime(r.getLong("alarmdate"), DateTimeZone.UTC)).toString("yyyy-MM-dd")
      ))

      val df = rdd.toDF()
      tmpCnt += 1L

      if (tmpCnt > Starter.flushCount) {
        val dfTemp = JobSessions.spark.sql("select * from allData")
        InsertToSTG(dfTemp)

        val dfEmpty = JobSessions.spark.sql("select * from allEmpty")
        dfEmpty.createOrReplaceTempView("allData")

        tmpCnt = 0L
      }
      val newdf = df.union(JobSessions.spark.sql("select * from allData"))
      newdf.createOrReplaceTempView("allData")

      cassSession.close()
    })

    val df = JobSessions.spark.sql("select * from allData")
    InsertToSTG(df)

    Starter.CleanupStaging(tableName)
    Starter.CleanupWarehouse(tableName)
    UpdateDWH
    Starter.CleanupSave(tableName)
  }

  def CreateDWHTable = {

    val colList =
      " nodeid string, " +
        " alarmtype string, " +
        " alarmdate bigint, " +
        " dateuuid string, " +
        " message string, " +
        " severitycode string, " +
        " ts bigint "

    JobSessions.spark.sql(
      " create table if not exists " + Starter.STG_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )

    JobSessions.spark.sql(
      " create table if not exists " + Starter.DWH_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )
  }

  def InsertToSTG (df: DataFrame) ={
    df.createOrReplaceTempView("stgDeviceAlarms")
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.STG_PREFIX+tableName + " partition (startday) " +
        " select " +
        " nodeid, " +
        " alarmtype, " +
        " alarmdate, " +
        " dateuuid, " +
        " message, " +
        " severitycode, " +
        " unix_timestamp() as ts, " +
        " startday " +
        " from stgDeviceAlarms "
    )
  }

  def UpdateDWH ={
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.DWH_PREFIX+tableName + " partition (startday) " +
        " select  nodeid, alarmtype, alarmdate, dateuuid, message, severitycode, ts, startday " +
        " from " +
        " ( " +
        " select " +
        " nodeid, " +
        " alarmtype, " +
        " alarmdate, " +
        " dateuuid, " +
        " message, " +
        " severitycode, " +
        " max(ts) over (partition by nodeid, alarmtype, alarmdate) as maxts, " +
        " ts, " +
        " startday " +
        " from " + Starter.STG_PREFIX+tableName +
        " where startday in ('" + Starter.filterPartitionYesterday + "', '" + Starter.filterPartitionToday + "') " +
        " ) a " +
        " where a.ts = a.maxts "
    )
  }

  def GetPartitions(from: String): DataFrame = {

    import JobSessions.spark.implicits._
    import scala.collection.convert.wrapAll._

    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()
    val tableName = "aggregation_filters"

    val stmt =
      " select obj from " + Starter.inp_cassKeyspace + "." + tableName +
        " where  type = 'connection_nodes' "

    val nodelist = cassSession.execute(stmt).all().toList

    val df = nodelist.map(r => new PartitionList(
      r.getString(0)
    )).toDF().distinct()

    cassSession.close()
    return df
  }

  /*
  def GetPartitions(from: String): DataFrame = {
    import JobSessions.spark.implicits._
    import scala.collection.convert.wrapAll._
    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()
    val stmt =
      " select nodeid " +
      " from " + Starter.inp_cassKeyspace + "." + tableName +
      " where alarmtype in " + alarmList + " " +
      " and   date >= maxTimeuuid('" + Starter.getStringFromMicrosecs(from.toLong) + "') " +
      " and   date <  maxTimeuuid('" + Starter.getStringFromMicrosecs(from.toLong+(1000000*60*60)) + "') " +
      " allow filtering "
    val nodelist = cassSession.execute(stmt).all().toList
    val df = nodelist.map(r => new PartitionList(
      r.getString(0)
    )).toDF().distinct()
    cassSession.close()
    return df
  }
  */

}


object ProcessCopyNeo {
  def run() = {

    SiteDataFromNeo.apply()
    val dfSite = JobSessions.spark.sql("select * from dfSite")
    try {
      dfSite.write.mode(SaveMode.ErrorIfExists).saveAsTable(Starter.DWH_PREFIX + "site")
    } catch {
      case ex: Exception => {
        JobSessions.spark.sql("insert overwrite table " + Starter.DWH_PREFIX + "site" + " select * from dfSite")
      }
    }

    FixtureDataFromNeo.apply()
    val dfFixture = JobSessions.spark.sql("select * from dfFixture")
    try {
      dfFixture.write.mode(SaveMode.ErrorIfExists).saveAsTable(Starter.DWH_PREFIX + "fixture")
    } catch {
      case ex: Exception => {
        JobSessions.spark.sql("insert overwrite table " + Starter.DWH_PREFIX + "fixture" + " select * from dfFixture")
      }
    }

    EnergySettingsFromNeo.apply()
    val dfEnergySettings = JobSessions.spark.sql("select * from dfEnergySettings")
    try {
      dfEnergySettings.write.mode(SaveMode.ErrorIfExists).saveAsTable(Starter.DWH_PREFIX + "energy_settings")
    } catch {
      case ex: Exception => {
        JobSessions.spark.sql("insert overwrite table " + Starter.DWH_PREFIX + "energy_settings" + " select * from dfEnergySettings ")
      }
    }

    NodeDataFromNeo.apply()
    val dfNode = JobSessions.spark.sql("select * from dfNode")
    try {
      dfNode.write.mode(SaveMode.ErrorIfExists).saveAsTable(Starter.DWH_PREFIX + "node")
    } catch {
      case ex: Exception => {
        JobSessions.spark.sql("insert overwrite table " + Starter.DWH_PREFIX + "node" + " select * from dfNode")
      }
    }
    val dfConnectionNodes = JobSessions.spark.sql("select 'connection_nodes' as type, nodeid as obj from dfNode")
    dfConnectionNodes.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_filters", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    CassandraDDL(Starter.inp_cassKeyspace)

    NodeHierDataFromNeo.apply()
    val dfNodeHier = JobSessions.spark.sql("select * from dfNodeHier ")
    try {
      dfNodeHier.write.mode(SaveMode.ErrorIfExists).saveAsTable(Starter.DWH_PREFIX + "orghierarchy_by_nodeid")
    } catch {
      case ex: Exception => {
        JobSessions.spark.sql("insert overwrite table " + Starter.DWH_PREFIX + "orghierarchy_by_nodeid" + " select * from dfNodeHier")
      }
    }
    dfNodeHier.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "orghierarchy_by_nodeid", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    CustomerDataFromNeo.apply()
    val dfCustomer = JobSessions.spark.sql("select * from dfCustomer")
    try {
      dfCustomer.write.mode(SaveMode.ErrorIfExists).saveAsTable(Starter.DWH_PREFIX + "customer")
    } catch {
      case ex: Exception => {
        JobSessions.spark.sql("insert overwrite table " + Starter.DWH_PREFIX + "customer" + " select * from dfCustomer")
      }
    }

    GroupDataFromNeo.apply()
    val dfGroup = JobSessions.spark.sql("select * from dfGroup")
    try {
      dfGroup.write.mode(SaveMode.ErrorIfExists).saveAsTable(Starter.DWH_PREFIX + "nodegroups")
    } catch {
      case ex: Exception => {
        JobSessions.spark.sql("insert overwrite table " + Starter.DWH_PREFIX + "nodegroups" + " select * from dfGroup")
      }
    }

    UserDataFromNeo.apply()
    val dfUser = JobSessions.spark.sql("select * from dfUser")
    try {
      dfUser.write.mode(SaveMode.ErrorIfExists).saveAsTable(Starter.DWH_PREFIX + "user")
    } catch {
      case ex: Exception => {
        JobSessions.spark.sql("insert overwrite table " + Starter.DWH_PREFIX + "user" + " select * from dfUser")
      }
    }

    PartnerDataFromNeo.apply()
    val dfPartner = JobSessions.spark.sql("select * from dfPartner")
    try {
      dfPartner.write.mode(SaveMode.ErrorIfExists).saveAsTable(Starter.DWH_PREFIX + "partner")
    } catch {
      case ex: Exception => {
        JobSessions.spark.sql("insert overwrite table " + Starter.DWH_PREFIX + "partner" + " select * from dfPartner")
      }
    }

    ParkingGroupDataFromNeo.apply()
    val dfParkingGroup = JobSessions.spark.sql("select * from dfParkingGroup")
    try {
      dfParkingGroup.write.mode(SaveMode.ErrorIfExists).saveAsTable(Starter.DWH_PREFIX + "parking_group")
    } catch {
      case ex: Exception => {
        JobSessions.spark.sql("insert overwrite table " + Starter.DWH_PREFIX + "parking_group" + " select * from dfParkingGroup")
      }
    }
    dfParkingGroup.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "parking_group", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    val dfParkingZoneGroup = JobSessions.spark.sql(
      " select t.* from " +
        " ( " +
        " select parkingzoneid, parkinggroupid " +
        " from   " + Starter.DWH_PREFIX + "parking_group lateral view explode(split(parkingzones,',')) parkingzones AS parkingzoneid " +
        " ) t " +
        " where length(t.parkingzoneid) > 0 "
    )
    dfParkingZoneGroup.createOrReplaceTempView("dfParkingZoneGroup")
    try {
      dfParkingZoneGroup.write.mode(SaveMode.ErrorIfExists).saveAsTable(Starter.DWH_PREFIX + "parking_zone_group")
    } catch {
      case ex: Exception => {
        JobSessions.spark.sql("insert overwrite table " + Starter.DWH_PREFIX + "parking_zone_group" + " select * from dfParkingZoneGroup")
      }
    }

    dfParkingZoneGroup.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "parking_zone_group", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

  }

  def CassandraDDL(cassKeySpace: String) = {
    //import scala.collection.convert.wrapAll._
    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

    val stmt1 =
      " create table if not exists "+ cassKeySpace + "." + "orghierarchy_by_nodeid ( " +
        " nodeid text, " +
        " orgid text, " +
        " siteid text, " +
        " nodename text, " +
        " orgname text, " +
        " sitename text, " +
        " siteaddress text, " +
        " bssid text, " +
        " nodehw text, " +
        " PRIMARY KEY(nodeid) " +
        " ) "
    cassSession.execute(stmt1)

    cassSession.close()
  }

}


object NodeDataFromNeo {

  import org.anormcypher._
  import play.api.libs.ws._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val wsclient = ning.NingWSClient()
  implicit val connection: Neo4jConnection = Neo4jREST(Starter.inp_neoIP, Starter.inp_neoPort.toInt, "neo4j", "neo4j1")

  def apply() = {

    try {
      val StructNeo = StructType(
        StructField("nodeid", StringType, true) ::
          StructField("siteid", StringType, true) ::
          StructField("fixtureid", StringType, true) ::
          StructField("fixtureType", StringType, true) ::
          StructField("fixturename", StringType, true) ::
          StructField("name", StringType, true) ::
          StructField("serialnumber", StringType, true) ::
          StructField("latitude", StringType, true) ::
          StructField("longitude", StringType, true) ::
          StructField("ip", StringType, true) ::
          StructField("mac", StringType, true) ::
          StructField("meshId", StringType, true) ::
          StructField("channel", StringType, true) ::
          StructField("baseStation", StringType, true) ::
          StructField("server", StringType, true) ::
          StructField("model", StringType, true) ::
          StructField("auth", StringType, true) ::
          StructField("softwareVersion", StringType, true) ::
          StructField("pdprofileid", StringType, true) ::
          StructField("signature", StringType, true) ::
          StructField("schedulename", StringType, true) ::
          StructField("building", StringType, true) ::
          StructField("bssid", StringType, true) ::
          StructField("configToken", StringType, true) ::
          StructField("pdprofilename", StringType, true) ::
          StructField("country_code", StringType, true) ::
          StructField("scheduleid", StringType, true) ::
          StructField("note", StringType, true) ::
          StructField("circuit", StringType, true) ::
          StructField("remoteNetwork", StringType, true) ::
          StructField("abcd", StringType, true) ::
          StructField("apn", StringType, true) ::
          StructField("publicKey", StringType, true) ::
          StructField("firmwareLastUpdated", StringType, true) ::
          StructField("time_zone", StringType, true) ::
          StructField("mfgDate", StringType, true) ::
          Nil)

      val allNeo = Cypher(" MATCH (n:Node) MATCH (n)-[BELONGS_TO]->(s:Site) where 'Active' in labels(n) " +
        //" and not n.nodeid =~ 'QAV.*' " +
        " return distinct " +
        " toString(n.nodeid) as nodeid," +
        " toString(s.siteid) as siteid," +
        " toString(n.fixtureid) as fixtureid," +
        " toString(n.fixtureType) as fixtureType," +
        " toString(n.fixturename) as fixturename," +
        " toString(n.name) as name," +
        " toString(n.serialnumber) as serialnumber, " +
        " toString(n.latitude) as latitude," +
        " toString(n.longitude) as longitude," +
        " toString(n.ip) as ip," +
        " toString(n.mac) as mac," +
        " toString(n.meshId) as meshId," +
        " toString(n.channel) as channel," +
        " toString(n.baseStation) as baseStation," +
        " toString(n.server) as server," +
        " toString(n.model) as model," +
        " toString(n.auth) as auth," +
        " toString(n.softwareVersion) as softwareVersion," +
        " toString(n.pdprofileid) as pdprofileid," +
        " toString(n.signature) as signature," +
        " toString(n.schedulename) as schedulename ," +
        " toString(n.building) as building," +
        " toString(n.bssid) as bssid," +
        " toString(n.configToken) as configToken," +
        " toString(n.pdprofilename) as pdprofilename," +
        " toString(n.country_code) as country_code," +
        " toString(n.scheduleid) as scheduleid," +
        " toString(n.note) as note," +
        " toString(n.circuit) as circuit," +
        " toString(n.remoteNetwork) as remoteNetwork," +
        " toString(n.abcd) as abcd," +
        " toString(n.apn) as apn," +
        " toString(n.publicKey) as publicKey," +
        " toString(n.firmwareLastUpdated) as firmwareLastUpdated," +
        " toString(n.time_zone) as time_zone," +
        " toString(n.mfgDate) as mfgDate "
      )

      val listNeo = allNeo.apply().map(row => {

        Row(
          row[Option[String]]("nodeid").map(_.toString).getOrElse(""),
          row[Option[String]]("siteid").map(_.toString).getOrElse(""),
          row[Option[String]]("fixtureid").map(_.toString).getOrElse(""),
          row[Option[String]]("fixtureType").map(_.toString).getOrElse(""),
          row[Option[String]]("fixturename").map(_.toString).getOrElse(""),
          row[Option[String]]("name").map(_.toString).getOrElse(""),
          row[Option[String]]("serialnumber").map(_.toString).getOrElse(""),
          row[Option[String]]("latitude").map(_.toString).getOrElse(""),
          row[Option[String]]("longitude").map(_.toString).getOrElse(""),
          row[Option[String]]("ip").map(_.toString).getOrElse(""),
          row[Option[String]]("mac").map(_.toString).getOrElse(""),
          row[Option[String]]("meshId").map(_.toString).getOrElse(""),
          row[Option[String]]("channel").map(_.toString).getOrElse(""),
          row[Option[String]]("baseStation").map(_.toString).getOrElse(""),
          row[Option[String]]("server").map(_.toString).getOrElse(""),
          row[Option[String]]("model").map(_.toString).getOrElse(""),
          row[Option[String]]("auth").map(_.toString).getOrElse(""),
          row[Option[String]]("softwareVersion").map(_.toString).getOrElse(""),
          row[Option[String]]("pdprofileid").map(_.toString).getOrElse(""),
          row[Option[String]]("signature").map(_.toString).getOrElse(""),
          row[Option[String]]("schedulename").map(_.toString).getOrElse(""),
          row[Option[String]]("building").map(_.toString).getOrElse(""),
          row[Option[String]]("bssid").map(_.toString).getOrElse(""),
          row[Option[String]]("configToken").map(_.toString).getOrElse(""),
          row[Option[String]]("pdprofilename").map(_.toString).getOrElse(""),
          row[Option[String]]("country_code").map(_.toString).getOrElse(""),
          row[Option[String]]("scheduleid").map(_.toString).getOrElse(""),
          row[Option[String]]("note").map(_.toString).getOrElse(""),
          row[Option[String]]("circuit").map(_.toString).getOrElse(""),
          row[Option[String]]("remoteNetwork").map(_.toString).getOrElse(""),
          row[Option[String]]("abcd").map(_.toString).getOrElse(""),
          row[Option[String]]("apn").map(_.toString).getOrElse(""),
          row[Option[String]]("publicKey").map(_.toString).getOrElse(""),
          row[Option[String]]("firmwareLastUpdated").map(_.toString).getOrElse(""),
          row[Option[String]]("time_zone").map(_.toString).getOrElse(""),
          row[Option[String]]("mfgDate").map(_.toString).getOrElse("")
        )
      }).toList

      val rddNeo = JobSessions.sc.makeRDD(listNeo)
      val dfNeo = JobSessions.spark.createDataFrame(rddNeo, StructNeo)

      Starter.rootLogger.info("Node -> Row count: " + dfNeo.count().toString)
      dfNeo.createOrReplaceTempView("dfNode")
    } catch {
      case ex: Exception => {
        println("Neo4j data pull exception (NodeDataFromNeo) : " + ex.getLocalizedMessage)
      }
    } finally {
      wsclient.close()
    }

  }

}


object UserDataFromNeo {

  import org.anormcypher._
  import play.api.libs.ws._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val wsclient = ning.NingWSClient()
  implicit val connection: Neo4jConnection = Neo4jREST(Starter.inp_neoIP, Starter.inp_neoPort.toInt, "neo4j", "neo4j1")

  def apply() = {

    try {
      val StructNeo = StructType(
        StructField("userid", StringType, true) ::
          StructField("name", StringType, true) ::
          StructField("email", StringType, true) ::
          StructField("roles", StringType, true) ::
          StructField("updated", StringType, true) ::
          StructField("created", StringType, true) ::
          StructField("orgid", StringType, true) ::
          Nil)

      val allNeo = Cypher(" MATCH (u:User) MATCH (u)-[IS_USER_OF]->(o:Org) where 'Active' in labels(u) " +
        " return distinct " +
        " toString(u.userid) as userid, " +
        " toString(u.name) as name, " +
        " toString(u.email) as email, " +
        " toString(u.roles) as roles, " +
        " toString(u.updated) as updated, " +
        " toString(u.created) as created, " +
        " toString(o.orgid) as orgid "
      )

      val listNeo = allNeo.apply().map(row => {

        Row(
          row[Option[String]]("userid").map(_.toString).getOrElse(""),
          row[Option[String]]("name").map(_.toString).getOrElse(""),
          row[Option[String]]("email").map(_.toString).getOrElse(""),
          row[Option[String]]("roles").map(_.toString).getOrElse(""),
          row[Option[String]]("updated").map(_.toString).getOrElse(""),
          row[Option[String]]("created").map(_.toString).getOrElse(""),
          row[Option[String]]("orgid").map(_.toString).getOrElse("")
        )
      }).toList

      val rddNeo = JobSessions.sc.makeRDD(listNeo)
      val dfNeo = JobSessions.spark.createDataFrame(rddNeo, StructNeo)

      Starter.rootLogger.info("User -> Row count: " + dfNeo.count().toString)
      dfNeo.createOrReplaceTempView("dfUser")
    } catch {
      case ex: Exception => {
        println("Neo4j data pull exception (UserDataFromNeo) : " + ex.getLocalizedMessage)
      }
    } finally {
      wsclient.close()
    }

  }

}


object PartnerDataFromNeo {

  import org.anormcypher._
  import play.api.libs.ws._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val wsclient = ning.NingWSClient()
  implicit val connection: Neo4jConnection = Neo4jREST(Starter.inp_neoIP, Starter.inp_neoPort.toInt, "neo4j", "neo4j1")

  def apply() = {

    try {
      val StructNeo = StructType(
        StructField("partner_orgid", StringType, true) ::
          StructField("customer_orgid", StringType, true) ::
          Nil)

      val allNeo = Cypher(" MATCH (p:Org) MATCH (p)-[HAS_CUSTOMER]->(c:Org) where p.type = 'partner' " +
        " return distinct " +
        " toString(p.orgid) as partner_orgid, " +
        " toString(c.orgid) as customer_orgid "
      )

      val listNeo = allNeo.apply().map(row => {

        Row(
          row[Option[String]]("partner_orgid").map(_.toString).getOrElse(""),
          row[Option[String]]("customer_orgid").map(_.toString).getOrElse("")
        )
      }).toList

      val rddNeo = JobSessions.sc.makeRDD(listNeo)
      val dfNeo = JobSessions.spark.createDataFrame(rddNeo, StructNeo)

      Starter.rootLogger.info("Partner -> Row count: " + dfNeo.count().toString)
      dfNeo.createOrReplaceTempView("dfPartner")
    } catch {
      case ex: Exception => {
        println("Neo4j data pull exception (PartnerDataFromNeo) : " + ex.getLocalizedMessage)
      }
    } finally {
      wsclient.close()
    }

  }

}


object ParkingGroupDataFromNeo {

  import org.anormcypher._
  import play.api.libs.ws._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val wsclient = ning.NingWSClient()
  implicit val connection: Neo4jConnection = Neo4jREST(Starter.inp_neoIP, Starter.inp_neoPort.toInt, "neo4j", "neo4j1")

  def apply() = {

    try {
      val StructNeo = StructType(
        StructField("parkinggroupid", StringType, true) ::
          StructField("name", StringType, true) ::
          StructField("description", StringType, true) ::
          StructField("parkingzones", StringType, true) ::
          StructField("vehicle_types", StringType, true) ::
          StructField("policy", StringType, true) ::
          StructField("created", StringType, true) ::
          Nil)

      val allNeo = Cypher(" MATCH (p:ParkingGroup) " +
        " return distinct " +
        " toString(p.parkinggroupid) as parkinggroupid, " +
        " toString(p.name) as name, " +
        " toString(p.description) as description, " +
        " toString(p.parkingzones) as parkingzones, " +
        " toString(p.vehicle_types) as vehicle_types, " +
        " toString(p.policy) as policy, " +
        " toString(p.created) as created "
      )

      val listNeo = allNeo.apply().map(row => {

        Row(
          row[Option[String]]("parkinggroupid").map(_.toString).getOrElse(""),
          row[Option[String]]("name").map(_.toString).getOrElse(""),
          row[Option[String]]("description").map(_.toString).getOrElse(""),
          row[Option[String]]("parkingzones").map(_.toString).getOrElse(""),
          row[Option[String]]("vehicle_types").map(_.toString).getOrElse(""),
          row[Option[String]]("policy").map(_.toString).getOrElse(""),
          row[Option[String]]("created").map(_.toString).getOrElse("")
        )
      }).toList

      val rddNeo = JobSessions.sc.makeRDD(listNeo)
      val dfNeo = JobSessions.spark.createDataFrame(rddNeo, StructNeo)

      Starter.rootLogger.info("ParkingGroup -> Row count: " + dfNeo.count().toString)
      dfNeo.createOrReplaceTempView("dfParkingGroup")
    } catch {
      case ex: Exception => {
        println("Neo4j data pull exception (ParkingGroupDataFromNeo) : " + ex.getLocalizedMessage)
      }
    } finally {
      wsclient.close()
    }

  }

}


object NodeHierDataFromNeo {

  import org.anormcypher._
  import play.api.libs.ws._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val wsclient = ning.NingWSClient()
  implicit val connection: Neo4jConnection = Neo4jREST(Starter.inp_neoIP, Starter.inp_neoPort.toInt, "neo4j", "neo4j1")

  def apply() = {

    try {
      val StructNeo = StructType(
        StructField("nodeid", StringType, true) ::
          StructField("orgid", StringType, true) ::
          StructField("siteid", StringType, true) ::
          StructField("nodename", StringType, true) ::
          StructField("orgname", StringType, true) ::
          StructField("sitename", StringType, true) ::
          StructField("siteaddress", StringType, true) ::
          StructField("bssid", StringType, true) ::
          StructField("nodehw", StringType, true) ::
          Nil)

      val allNeo = Cypher(
        " MATCH (n:Node) " +
          " OPTIONAL MATCH (n)-[:BELONGS_TO]->(s:Site)-[:BELONGS_TO]->(o:Org) " +
          " OPTIONAL MATCH (sch:Schedule)-[:HAS]->(n) " +
          " OPTIONAL MATCH (c:Config)-[:BELONGS_TO]->(n) " +
          " RETURN " +
          "   toString(n.nodeid) as nodeid, " +
          "   toString(o.orgid) as orgid, " +
          "   toString(s.siteid) as siteid, " +
          "   toString(n.name) as nodename, " +
          "   toString(o.name) as orgname, " +
          "   toString(s.name) as sitename, " +
          "   toString(s.address) as siteaddress, " +
          "   toString(n.bssid) as bssid, " +
          "   toString(n.model) as nodehw " +
          "  "
      )

      val listNeo = allNeo.apply().map(row => {

        Row(
          row[Option[String]]("nodeid").map(_.toString).getOrElse(""),
          row[Option[String]]("orgid").map(_.toString).getOrElse(""),
          row[Option[String]]("siteid").map(_.toString).getOrElse(""),
          row[Option[String]]("nodename").map(_.toString).getOrElse(""),
          row[Option[String]]("orgname").map(_.toString).getOrElse(""),
          row[Option[String]]("sitename").map(_.toString).getOrElse(""),
          row[Option[String]]("siteaddress").map(_.toString).getOrElse(""),
          row[Option[String]]("bssid").map(_.toString).getOrElse(""),
          row[Option[String]]("nodehw").map(_.toString).getOrElse("")
        )
      }).toList

      val rddNeo = JobSessions.sc.makeRDD(listNeo)
      val dfNeo = JobSessions.spark.createDataFrame(rddNeo, StructNeo)

      Starter.rootLogger.info("NodeHier -> Row count: " + dfNeo.count().toString)
      dfNeo.createOrReplaceTempView("dfNodeHier")
    } catch {
      case ex: Exception => {
        println("Neo4j data pull exception (NodeHierDataFromNeo) : " + ex.getLocalizedMessage)
      }
    } finally {
      wsclient.close()
    }

  }

}


object SiteDataFromNeo {

  import org.anormcypher._
  import play.api.libs.ws._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val wsclient = ning.NingWSClient()
  implicit val connection: Neo4jConnection = Neo4jREST(Starter.inp_neoIP, Starter.inp_neoPort.toInt, "neo4j", "neo4j1")

  def apply() = {

    try {

      val StructNeo = StructType(
        StructField("siteid", StringType, true) ::
          StructField("orgid", StringType, true) ::
          StructField("name", StringType, true) ::
          StructField("latitude", StringType, true) ::
          StructField("longitude", StringType, true) ::
          StructField("street1", StringType, true) ::
          StructField("street2", StringType, true) ::
          StructField("city", StringType, true) ::
          StructField("state", StringType, true) ::
          StructField("postal_code", StringType, true) ::
          StructField("country", StringType, true) ::
          StructField("region_code", StringType, true) ::
          StructField("time_zone", StringType, true) ::
          StructField("altitude", StringType, true) ::
          StructField("created", StringType, true) ::
          Nil)

      val allNeo = Cypher(" MATCH (s:Site) OPTIONAL MATCH (s)-[BELONGS_TO]->(o:Org) " +
        " RETURN " +
        " toString(s.siteid) as siteid, " +
        " toString(o.orgid) as orgid, " +
        " toString(s.name) as name, " +
        " toString(s.latitude) as latitude, " +
        " toString(s.longitude) as longitude, " +
        " toString(s.street1) as street1, " +
        " toString(s.street2) as street2, " +
        " toString(s.city) as city, " +
        " toString(s.state) as state, " +
        " toString(s.postal_code) as postal_code, " +
        " toString(s.country) as country, " +
        " toString(s.region_code) as region_code, " +
        " toString(s.time_zone) as time_zone, " +
        " toString(s.altitude) as altitude, " +
        " toString(s.created) as created "
      )

      val listNeo = allNeo.apply().map(row => {
        Row(
          row[Option[String]]("siteid").map(_.toString).getOrElse(""),
          row[Option[String]]("orgid").map(_.toString).getOrElse(""),
          row[Option[String]]("name").map(_.toString).getOrElse(""),
          row[Option[String]]("latitude").map(_.toString).getOrElse(""),
          row[Option[String]]("longitude").map(_.toString).getOrElse(""),
          row[Option[String]]("street1").map(_.toString).getOrElse(""),
          row[Option[String]]("street2").map(_.toString).getOrElse(""),
          row[Option[String]]("city").map(_.toString).getOrElse(""),
          row[Option[String]]("state").map(_.toString).getOrElse(""),
          row[Option[String]]("postal_code").map(_.toString).getOrElse(""),
          row[Option[String]]("country").map(_.toString).getOrElse(""),
          row[Option[String]]("region_code").map(_.toString).getOrElse(""),
          row[Option[String]]("time_zone").map(_.toString).getOrElse(""),
          row[Option[String]]("altitude").map(_.toString).getOrElse(""),
          row[Option[String]]("created").map(_.toString).getOrElse("")
        )
      }).toList

      val rddNeo = JobSessions.sc.makeRDD(listNeo)
      val dfNeo = JobSessions.spark.createDataFrame(rddNeo, StructNeo)

      Starter.rootLogger.info("Site -> Row count: " + dfNeo.count().toString)
      dfNeo.createOrReplaceTempView("dfSite")
    } catch {
      case ex: Exception => {
        println("Neo4j data pull exception (SiteDataFromNeo) : " + ex.getLocalizedMessage)
      }
    } finally {
      wsclient.close()
    }

  }

}


object EnergySettingsFromNeo {

  import org.anormcypher._
  import play.api.libs.ws._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val wsclient = ning.NingWSClient()
  implicit val connection: Neo4jConnection = Neo4jREST(Starter.inp_neoIP, Starter.inp_neoPort.toInt, "neo4j", "neo4j1")

  def apply() = {

    try {
      val StructNeo = StructType(
        StructField("siteid", StringType, true) ::
          StructField("nodeid", StringType, true) ::
          StructField("fixtureid", StringType, true) ::
          StructField("legacy_power", StringType, true) ::
          StructField("max_led_power", StringType, true) ::
          Nil)

      val allNeo = Cypher(" MATCH (s:Site)-[:HAS]-> (f:Fixture) -[:HAS]-> (n:Node) " +
        " WHERE  toFloat(replace(replace(f.LegacyPowerDraw,\"W\",\"\"),\"w\",\"\")) > toFloat(replace(replace(f.PowerDraw,\"W\",\"\"),\"w\",\"\")) " +
        " RETURN s.siteid as siteid, n.nodeid as nodeid, f.fixtureid as fixtureid, toString(f.LegacyPowerDraw) as legacy_power, toString(f.PowerDraw) as max_led_power "
      )

      val listNeo = allNeo.apply().map(row => {
        Row(
          row[Option[String]]("siteid").map(_.toString).getOrElse(""),
          row[Option[String]]("nodeid").map(_.toString).getOrElse(""),
          row[Option[String]]("fixtureid").map(_.toString).getOrElse(""),
          row[Option[String]]("legacy_power").map(_.toString).getOrElse(""),
          row[Option[String]]("max_led_power").map(_.toString).getOrElse("")
        )
      }).toList

      val rddNeo = JobSessions.sc.makeRDD(listNeo)
      val dfNeo = JobSessions.spark.createDataFrame(rddNeo, StructNeo)

      Starter.rootLogger.info("EnergySettings -> Row count: " + dfNeo.count().toString)
      dfNeo.createOrReplaceTempView("dfEnergySettings")
    } catch {
      case ex: Exception => {
        println("Neo4j data pull exception (EnergySettingsFromNeo) : " + ex.getLocalizedMessage)
      }
    } finally {
      wsclient.close()
    }

  }

}



object FixtureDataFromNeo {

  import org.anormcypher._
  import play.api.libs.ws._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val wsclient = ning.NingWSClient()
  implicit val connection: Neo4jConnection = Neo4jREST(Starter.inp_neoIP, Starter.inp_neoPort.toInt, "neo4j", "neo4j1")

  def apply() = {

    try {
      val StructNeo = StructType(
        StructField("name", StringType, true) ::
          StructField("fixtureid", StringType, true) ::
          StructField("MinPower100", StringType, true) ::
          StructField("MinPower10", StringType, true) ::
          StructField("description", StringType, true) ::
          StructField("manufacturersku", StringType, true) ::
          StructField("MinPower50", StringType, true) ::
          StructField("fixtureType", StringType, true) ::
          StructField("MaxPower10", StringType, true) ::
          StructField("nemasocket", StringType, true) ::
          StructField("MinimumLightLevelForFailureDetection", StringType, true) ::
          StructField("MaxPower0", StringType, true) ::
          StructField("MinPower0", StringType, true) ::
          StructField("manufacturer", StringType, true) ::
          StructField("PowerDraw", StringType, true) ::
          StructField("MaxPower100", StringType, true) ::
          StructField("MaxPower50", StringType, true) ::
          StructField("BallastCost", StringType, true) ::
          StructField("BulbCost", StringType, true) ::
          StructField("LegacyPowerDraw", StringType, true) ::
          StructField("DailyOperatingTime", StringType, true) ::
          StructField("site", StringType, true) ::
          Nil)

      val allNeo = Cypher(" MATCH (f:Fixture) " +
        " RETURN " +
        " toString(f.name) as name, " +
        " toString(f.fixtureid) as fixtureid, " +
        " toString(f.MinPower100) as MinPower100, " +
        " toString(f.MinPower10) as MinPower10, " +
        " toString(f.description) as description, " +
        " toString(f.manufacturersku) as manufacturersku, " +
        " toString(f.MinPower50) as MinPower50, " +
        " toString(f.fixtureType) as fixtureType, " +
        " toString(f.MaxPower10) as MaxPower10, " +
        " toString(f.nemasocket) as nemasocket, " +
        " toString(f.MinimumLightLevelForFailureDetection) as MinimumLightLevelForFailureDetection, " +
        " toString(f.MaxPower0) as MaxPower0, " +
        " toString(f.MinPower0) as MinPower0, " +
        " toString(f.manufacturer) as manufacturer, " +
        " toString(f.PowerDraw) as PowerDraw, " +
        " toString(f.MaxPower100) as MaxPower100, " +
        " toString(f.MaxPower50) as MaxPower50, " +
        " toString(f.BallastCost) as BallastCost, " +
        " toString(f.BulbCost) as BulbCost, " +
        " toString(f.LegacyPowerDraw) as LegacyPowerDraw, " +
        " toString(f.DailyOperatingTime) as DailyOperatingTime, " +
        " toString(f.site) as site "
      )

      val listNeo = allNeo.apply().map(row => {
        Row(
          row[Option[String]]("name").map(_.toString).getOrElse(""),
          row[Option[String]]("fixtureid").map(_.toString).getOrElse(""),
          row[Option[String]]("MinPower100").map(_.toString).getOrElse(""),
          row[Option[String]]("MinPower10").map(_.toString).getOrElse(""),
          row[Option[String]]("description").map(_.toString).getOrElse(""),
          row[Option[String]]("manufacturersku").map(_.toString).getOrElse(""),
          row[Option[String]]("MinPower50").map(_.toString).getOrElse(""),
          row[Option[String]]("fixtureType").map(_.toString).getOrElse(""),
          row[Option[String]]("MaxPower10").map(_.toString).getOrElse(""),
          row[Option[String]]("nemasocket").map(_.toString).getOrElse(""),
          row[Option[String]]("MinimumLightLevelForFailureDetection").map(_.toString).getOrElse(""),
          row[Option[String]]("MaxPower0").map(_.toString).getOrElse(""),
          row[Option[String]]("MinPower0").map(_.toString).getOrElse(""),
          row[Option[String]]("manufacturer").map(_.toString).getOrElse(""),
          row[Option[String]]("PowerDraw").map(_.toString).getOrElse(""),
          row[Option[String]]("MaxPower100").map(_.toString).getOrElse(""),
          row[Option[String]]("MaxPower50").map(_.toString).getOrElse(""),
          row[Option[String]]("BallastCost").map(_.toString).getOrElse(""),
          row[Option[String]]("BulbCost").map(_.toString).getOrElse(""),
          row[Option[String]]("LegacyPowerDraw").map(_.toString).getOrElse(""),
          row[Option[String]]("DailyOperatingTime").map(_.toString).getOrElse(""),
          row[Option[String]]("site").map(_.toString).getOrElse("")
        )
      }).toList

      val rddNeo = JobSessions.sc.makeRDD(listNeo)
      val dfNeo = JobSessions.spark.createDataFrame(rddNeo, StructNeo)

      Starter.rootLogger.info("Fixture -> Row count: " + dfNeo.count().toString)
      dfNeo.createOrReplaceTempView("dfFixture")
    } catch {
      case ex: Exception => {
        println("Neo4j data pull exception (FixtureDataFromNeo) : " + ex.getLocalizedMessage)
      }
    } finally {
      wsclient.close()
    }

  }

}


object CustomerDataFromNeo {

  import org.anormcypher._
  import play.api.libs.ws._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val wsclient = ning.NingWSClient()
  implicit val connection: Neo4jConnection = Neo4jREST(Starter.inp_neoIP, Starter.inp_neoPort.toInt, "neo4j", "neo4j1")

  def apply() = {

    try {

      val StructNeo = StructType(
        StructField("orgid", StringType, true) ::
          StructField("name", StringType, true) ::
          StructField("type", StringType, true) ::
          StructField("contact_name", StringType, true) ::
          StructField("contact_phone", StringType, true) ::
          StructField("contact_email", StringType, true) ::
          StructField("street1", StringType, true) ::
          StructField("street2", StringType, true) ::
          StructField("city", StringType, true) ::
          StructField("state", StringType, true) ::
          StructField("postal_code", StringType, true) ::
          StructField("country", StringType, true) ::
          StructField("created", StringType, true) ::
          Nil)

      val allNeo = Cypher(" MATCH (c:Org) MATCH (c)-[IS_CUSTOMER_OF]->(o:Org) " +
        " RETURN DISTINCT " +
        " toString(c.orgid) as orgid, " +
        " toString(c.name) as name, " +
        " toString(c.type) as type, " +
        " toString(c.contact_name) as contact_name, " +
        " toString(c.contact_phone) as contact_phone, " +
        " toString(c.contact_email) as contact_email, " +
        " toString(c.street1) as street1, " +
        " toString(c.street2) as street2, " +
        " toString(c.city) as city, " +
        " toString(c.state) as state, " +
        " toString(c.postal_code) as postal_code, " +
        " toString(c.country) as country, " +
        " toString(c.created) as created "
      )

      val listNeo = allNeo.apply().map(row => {
        Row(
          row[Option[String]]("orgid").map(_.toString).getOrElse(""),
          row[Option[String]]("name").map(_.toString).getOrElse(""),
          row[Option[String]]("type").map(_.toString).getOrElse(""),
          row[Option[String]]("contact_name").map(_.toString).getOrElse(""),
          row[Option[String]]("contact_phone").map(_.toString).getOrElse(""),
          row[Option[String]]("contact_email").map(_.toString).getOrElse(""),
          row[Option[String]]("street1").map(_.toString).getOrElse(""),
          row[Option[String]]("street2").map(_.toString).getOrElse(""),
          row[Option[String]]("city").map(_.toString).getOrElse(""),
          row[Option[String]]("state").map(_.toString).getOrElse(""),
          row[Option[String]]("postal_code").map(_.toString).getOrElse(""),
          row[Option[String]]("country").map(_.toString).getOrElse(""),
          row[Option[String]]("created").map(_.toString).getOrElse("")
        )
      }).toList

      val rddNeo = JobSessions.sc.makeRDD(listNeo)
      val dfNeo = JobSessions.spark.createDataFrame(rddNeo, StructNeo)

      Starter.rootLogger.info("Customer -> Row count: " + dfNeo.count().toString)
      dfNeo.createOrReplaceTempView("dfCustomer")
    } catch {
      case ex: Exception => {
        println("Neo4j data pull exception (CustomerDataFromNeo) : " + ex.getLocalizedMessage)
      }
    } finally {
      wsclient.close()
    }

  }

}


object GroupDataFromNeo {

  import org.anormcypher._
  import play.api.libs.ws._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val wsclient = ning.NingWSClient()
  implicit val connection: Neo4jConnection = Neo4jREST(Starter.inp_neoIP, Starter.inp_neoPort.toInt, "neo4j", "neo4j1")

  def apply() = {

    try {

      val StructNeo = StructType(
        StructField("siteid", StringType, true) ::
          StructField("groupid", StringType, true) ::
          StructField("groupname", StringType, true) ::
          StructField("groupdesc", StringType, true) ::
          StructField("grouptype", StringType, true) ::
          StructField("nodeid", StringType, true) ::
          StructField("labellist", StringType, true) ::
          Nil)

      val allNeo = Cypher(
        " MATCH (s:Site)-[:HAS]-> (g:Group) -[:HAS]-> (n:Node) " +
          " WHERE NOT 'LightingGroup' IN labels(g) " +
          " AND   NOT 'ParkingGroup' IN labels(g) " +
          " RETURN DISTINCT " +
          " toString(s.siteid) as siteid, " +
          " toString(g.groupid) as groupid, " +
          " toString(g.name) as groupname, " +
          " toString(g.description) as groupdesc, " +
          " toString('Organizational') as grouptype, " +
          " toString(n.nodeid) as nodeid, " +
          " toString(reduce(output='', l in labels(g) | output+l+' ')) as labellist " +
          " UNION ALL " +
          " MATCH (s:Site)-[:HAS]-> (g:Group) -[:HAS]-> (n:Node) " +
          " WHERE 'LightingGroup' IN labels(g) " +
          " RETURN " +
          " toString(s.siteid) as siteid, " +
          " toString(g.groupid) as groupid, " +
          " toString(g.name) as groupname, " +
          " toString(g.description) as groupdesc, " +
          " toString('Lighting') as grouptype, " +
          " toString(n.nodeid) as nodeid, " +
          " toString(reduce(output='', l in labels(g) | output+l+' ')) as labellist "
      )

      val listNeo = allNeo.apply().map(row => {
        Row(
          row[Option[String]]("siteid").map(_.toString).getOrElse(""),
          row[Option[String]]("groupid").map(_.toString).getOrElse(""),
          row[Option[String]]("groupname").map(_.toString).getOrElse(""),
          row[Option[String]]("groupdesc").map(_.toString).getOrElse(""),
          row[Option[String]]("grouptype").map(_.toString).getOrElse(""),
          row[Option[String]]("nodeid").map(_.toString).getOrElse(""),
          row[Option[String]]("labellist").map(_.toString).getOrElse("")
        )
      }).toList

      val rddNeo = JobSessions.sc.makeRDD(listNeo)
      val dfNeo = JobSessions.spark.createDataFrame(rddNeo, StructNeo)

      Starter.rootLogger.info("Group -> Row count: " + dfNeo.count().toString)
      dfNeo.createOrReplaceTempView("dfGroup")
    } catch {
      case ex: Exception => {
        println("Neo4j data pull exception (GroupDataFromNeo) : " + ex.getLocalizedMessage)
      }
    } finally {
      wsclient.close()
    }

  }

}


object ProcessConnectionStatus {

  case class PartitionList(nodeid:String)

  case class ConnectionStatus(
                               nodeid: String, since:Long, isConnected:Boolean, startday:String
                             )

  val tableName = "connection_status"
  val rootLogger = Logger.getRootLogger

  def run(startDt: DateTime, untilDt: DateTime, cassKeySpace: String) = {

    import JobSessions.spark.implicits._

    CreateDWHTable

    JobSessions.spark.emptyDataset[ConnectionStatus].toDF().createOrReplaceTempView("allData")
    JobSessions.spark.emptyDataset[ConnectionStatus].toDF().createOrReplaceTempView("allEmpty")

    val from = (startDt.getMillis * 1000).toString
    val until = (untilDt.getMillis * 1000).toString
    var tmpCnt = 0L

    GetPartitions(from).collect().foreach(r=> {
      var nodeid = r.getString(0)

      import scala.collection.convert.wrapAll._
      val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

      val stmt =
        " select nodeid, unixTimestampOf(since) as since, isconnected " +
          " from " + Starter.inp_cassKeyspace + "." + tableName +
          " where  nodeid = '" + nodeid + "' " +
          " and    since >= maxTimeuuid('" + Starter.getStringFromMicrosecs(from.toLong) + "') " +
          " and    since <  maxTimeuuid('" + Starter.getStringFromMicrosecs(until.toLong) + "') " +
          " limit " + Starter.rowLimit.toString +
          " allow  filtering "

      val cassData = cassSession.execute(stmt).all().toList

      val rdd = cassData.map(r => new ConnectionStatus(
        r.getString("nodeid"),
        r.getLong("since"),
        r.getBool("isconnected"),
        (new DateTime(r.getLong("since"), DateTimeZone.UTC)).toString("yyyy-MM-dd")
      ))

      val df = rdd.toDF()
      tmpCnt += 1L

      if (tmpCnt > Starter.flushCount) {
        val dfTemp = JobSessions.spark.sql("select * from allData")
        InsertToSTG(dfTemp)

        val dfEmpty = JobSessions.spark.sql("select * from allEmpty")
        dfEmpty.createOrReplaceTempView("allData")

        tmpCnt = 0L
      }
      val newdf = df.union(JobSessions.spark.sql("select * from allData"))
      newdf.createOrReplaceTempView("allData")

      cassSession.close()
    })

    val df = JobSessions.spark.sql("select * from allData")
    InsertToSTG(df)

    Starter.CleanupStaging(tableName)
    Starter.CleanupWarehouse(tableName)
    UpdateDWH
    Starter.CleanupSave(tableName)
  }

  def CreateDWHTable = {

    val colList =
      " nodeid string, " +
        " since bigint, " +
        " isconnected boolean, " +
        " ts bigint "

    JobSessions.spark.sql(
      " create table if not exists " + Starter.STG_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )

    JobSessions.spark.sql(
      " create table if not exists " + Starter.DWH_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )
  }

  def InsertToSTG (df: DataFrame) ={
    df.createOrReplaceTempView("stgConnectionStatus")
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.STG_PREFIX+tableName + " partition (startday) " +
        " select " +
        " nodeid, " +
        " since, " +
        " isconnected, " +
        " unix_timestamp() as ts, " +
        " startday " +
        " from stgConnectionStatus "
    )
  }

  def UpdateDWH ={
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.DWH_PREFIX+tableName + " partition (startday) " +
        " select  nodeid, since, isconnected, ts, startday " +
        " from " +
        " ( " +
        " select " +
        " nodeid, " +
        " since, " +
        " isconnected, " +
        " max(ts) over (partition by nodeid, since) as maxts, " +
        " ts, " +
        " startday " +
        " from " + Starter.STG_PREFIX+tableName +
        " where startday in ('" + Starter.filterPartitionYesterday + "', '" + Starter.filterPartitionToday + "') " +
        " ) a " +
        " where a.ts = a.maxts "
    )
  }

  /*
  def GetPartitions(from: String): DataFrame = {
    import JobSessions.spark.implicits._
    import scala.collection.convert.wrapAll._
    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()
    val stmt =
      " select nodeid " +
      " from " + Starter.inp_cassKeyspace + "." + tableName +
      " where  since >= maxTimeuuid('" + Starter.getStringFromMicrosecs(from.toLong) + "') " +
//      " and    since <  maxTimeuuid('" + Starter.getStringFromMicrosecs(from.toLong+(1000000*60*60)) + "') " +
      " allow filtering "
    val nodelist = cassSession.execute(stmt).all().toList
    val df = nodelist.map(r => new PartitionList(
      r.getString(0)
    )).toDF().distinct()
    cassSession.close()
    return df
  }
  */

  def GetPartitions(from: String): DataFrame = {

    import JobSessions.spark.implicits._
    import scala.collection.convert.wrapAll._

    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()
    val tableName = "aggregation_filters"

    val stmt =
      " select obj from " + Starter.inp_cassKeyspace + "." + tableName +
        " where  type = 'connection_nodes' "

    val nodelist = cassSession.execute(stmt).all().toList

    val df = nodelist.map(r => new PartitionList(
      r.getString(0)
    )).toDF().distinct()

    cassSession.close()
    return df
  }

}


object ProcessParkingHistoric {

  case class PartitionList(siteid:String)

  case class ParkingSpotHistoric(
                                  siteid:String, since:Long, channel:Int, nodeid:String, objectid:String,
                                  occupancy:Boolean, orgid:String, parkinggroupid:String, parkingspotid:String, parkingzoneid:String
                                )

  val tableName = "parking_spot_historic"
  val rootLogger = Logger.getRootLogger

  def run(startDt: DateTime, untilDt: DateTime, cassKeySpace: String) = {

    import JobSessions.spark.implicits._

    CreateDWHTable

    JobSessions.spark.emptyDataset[ParkingSpotHistoric].toDF().createOrReplaceTempView("allData")
    JobSessions.spark.emptyDataset[ParkingSpotHistoric].toDF().createOrReplaceTempView("allEmpty")

    val from = (startDt.getMillis * 1000).toString
    val until = (untilDt.getMillis * 1000).toString
    var tmpCnt = 0L

    val DFsites = GetPartitions(from)
    val DFsiteGroup = Starter.pivotGroup(DFsites, 10, "String")

    DFsiteGroup.collect().foreach(r=> {
      var siteidGrp = r.getString(0)

      import scala.collection.convert.wrapAll._
      val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

      val stmt =
        " select siteid,since,channel,nodeid,objectid,occupancy,orgid,parkinggroupid,parkingspotid,parkingzoneid " +
          " from " + Starter.inp_cassKeyspace + "." + tableName +
          " where  siteid in (" + siteidGrp + ") " +
          " and    since >= " + from +
          " and    since <  " + until +
          " limit " + Starter.rowLimit.toString + " "
          //" allow  filtering "

      Starter.rootLogger.info("stmt = "+ stmt)

      val cassData = cassSession.execute(stmt).all().toList

      val rdd = cassData.map(r => new ParkingSpotHistoric(
        r.getString("siteid"),
        r.getLong("since"),
        r.getInt("channel"),
        r.getString("nodeid"),
        r.getString("objectid"),
        r.getBool("occupancy"),
        r.getString("orgid"),
        r.getString("parkinggroupid"),
        r.getString("parkingspotid"),
        r.getString("parkingzoneid")
      ))

      val df = rdd.toDF()
      tmpCnt += 1L

      if (tmpCnt > Starter.flushCount) {
        val dfTemp = JobSessions.spark.sql("select * from allData")
        InsertToSTG(dfTemp)

        val dfEmpty = JobSessions.spark.sql("select * from allEmpty")
        dfEmpty.createOrReplaceTempView("allData")

        tmpCnt = 0L
      }
      val newdf = df.union(JobSessions.spark.sql("select * from allData"))
      newdf.createOrReplaceTempView("allData")

      cassSession.close()
    })

    val df = JobSessions.spark.sql("select * from allData")

    Starter.rootLogger.info("dfcnt="+ df.count())

    InsertToSTG(df)

    Starter.CleanupStaging(tableName) // manual
    Starter.CleanupWarehouse(tableName) // manual
    UpdateDWH // manual
    Starter.CleanupSave(tableName)
  }

  def CreateDWHTable = {

    val colList =
      " siteid string, " +
        " since bigint, " +
        " channel int, " +
        " nodeid string, " +
        " objectid string, " +
        " occupancy boolean, " +
        " orgid string, " +
        " parkinggroupid string, " +
        " parkingspotid string, " +
        " parkingzoneid string, " +
        " ts bigint "

    JobSessions.spark.sql(
      " create table if not exists " + Starter.STG_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        //" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
        " STORED AS ORC "
    )

    JobSessions.spark.sql(
      " create table if not exists " + Starter.DWH_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        //" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
        " STORED AS ORC "
    )
  }

  def InsertToSTG (df: DataFrame) ={
    df.createOrReplaceTempView("stgParkingSpotHistoric")
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.STG_PREFIX+tableName + " partition (startday) " +
        " select " +
        " siteid, " +
        " since, " +
        " channel, " +
        " nodeid, " +
        " objectid, " +
        " occupancy, " +
        " orgid, " +
        " parkinggroupid, " +
        " parkingspotid, " +
        " parkingzoneid, " +
        " unix_timestamp() as ts, " +
        " substr(from_unixtime(cast(since/1000000 as bigint)), 1, 10) as startday " +
        " from stgParkingSpotHistoric "
    )
  }

  def UpdateDWH ={
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.DWH_PREFIX+tableName + " partition (startday) " +
        " select  siteid,since,channel,nodeid,objectid,occupancy,orgid,parkinggroupid,parkingspotid,parkingzoneid, ts, startday " +
        " from " +
        " ( " +
        " select " +
        " siteid, " +
        " since, " +
        " channel, " +
        " nodeid, " +
        " objectid, " +
        " occupancy, " +
        " orgid, " +
        " parkinggroupid, " +
        " parkingspotid, " +
        " parkingzoneid, " +
        " max(ts) over (partition by siteid, since) as maxts, " +
        " ts, " +
        " startday " +
        " from " + Starter.STG_PREFIX+tableName +
        " where startday in ('" + Starter.filterPartitionYesterday + "', '" + Starter.filterPartitionToday + "') " +
        " ) a " +
        " where a.ts = a.maxts "
    )
  }

  def UpdateDWHUTC ={
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.DWH_PREFIX+tableName + " partition (startday) " +
        " select  siteid,since,channel,nodeid,objectid,occupancy,orgid,parkinggroupid,parkingspotid,parkingzoneid, ts, startday " +
        " from " +
        " ( " +
        " select " +
        " siteid, " +
        " since, " +
        " channel, " +
        " nodeid, " +
        " objectid, " +
        " occupancy, " +
        " orgid, " +
        " parkinggroupid, " +
        " parkingspotid, " +
        " parkingzoneid, " +
        " max(ts) over (partition by siteid, since) as maxts, " +
        " ts, " +
        " startday " +
        " from " + Starter.STG_PREFIX+tableName +
        " where startday in ('" + Starter.filterPartitionToday + "') " +
        " ) a " +
        " where a.ts = a.maxts "
    )
  }

  def GetPartitions(from: String): DataFrame = {

    import JobSessions.spark.implicits._
    import scala.collection.convert.wrapAll._

    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()
    val tableName = "parking_spot_historic"

    val stmt =
      " select distinct siteid from " + Starter.inp_cassKeyspace + "." + tableName

    val sitelist = cassSession.execute(stmt).all().toList

    val df = sitelist.map(r => new PartitionList(
      r.getString(0)
    )).toDF()

    cassSession.close()
    return df
  }

}


object ProcessAggregationConnectionNode {

  case class PartitionList(nodeid: String)

  case class AggregationConnectionNode(
                                        nodeid: String, starttime: Long, conduration: Long, conpercent: Double, disconnects: Int,
                                        enddt: String, endtime: Long, startday: String, startdt: String,
                                        starthr: String, ts: Long
                                      )

  val tableName = "aggregation_connection_node"
  val rootLogger = Logger.getRootLogger

  def run(startDt: DateTime, untilDt: DateTime, cassKeySpace: String) = {

    import JobSessions.spark.implicits._

    CreateDWHTable

    JobSessions.spark.emptyDataset[AggregationConnectionNode].toDF().createOrReplaceTempView("allData")
    JobSessions.spark.emptyDataset[AggregationConnectionNode].toDF().createOrReplaceTempView("allEmpty")

    val from = (startDt.getMillis * 1000).toString
    val until = (untilDt.getMillis * 1000).toString
    var tmpCnt = 0L

    GetPartitions(from).collect().foreach(r=> {
      var nodeid = r.getString(0)

      import scala.collection.convert.wrapAll._
      val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

      val stmt =
        " select nodeid,starttime,conduration,conpercent,disconnects,enddt,endtime,startday,startdt,starthr,ts " +
          " from " + Starter.inp_cassKeyspace + "." + tableName +
          " where  nodeid = '" + nodeid + "' " +
          " and    starttime >= " + from + " " +
          " and    starttime <  " + until + " " +
          " allow  filtering "

      val cassData = cassSession.execute(stmt).all().toList

      val rdd = cassData.map(r => new AggregationConnectionNode(
        r.getString("nodeid"),
        r.getLong("starttime"),
        r.getLong("conduration"),
        r.getDouble("conpercent"),
        r.getInt("disconnects"),
        r.getString("enddt"),
        r.getLong("endtime"),
        r.getString("startday"),
        r.getString("startdt"),
        r.getString("starthr"),
        r.getLong("ts")
      ))

      val df = rdd.toDF()
      tmpCnt += 1L

      if (tmpCnt > Starter.flushCount) {
        val dfTemp = JobSessions.spark.sql("select * from allData")
        InsertToSTG(dfTemp)

        val dfEmpty = JobSessions.spark.sql("select * from allEmpty")
        dfEmpty.createOrReplaceTempView("allData")

        tmpCnt = 0L
      }
      val newdf = df.union(JobSessions.spark.sql("select * from allData"))
      newdf.createOrReplaceTempView("allData")

      cassSession.close()
    })

    val df = JobSessions.spark.sql("select * from allData")
    InsertToSTG(df)

    Starter.CleanupStaging(tableName)
    Starter.CleanupWarehouse(tableName)
    UpdateDWH
    Starter.CleanupSave(tableName)
  }

  def CreateDWHTable = {

    val colList =
      " nodeid string, " +
        " starttime bigint, " +
        " conduration bigint, " +
        " conpercent double, " +
        " disconnects int, " +
        " enddt string, " +
        " endtime bigint, " +
        " startdt string, " +
        " starthr string, " +
        " ts bigint "

    JobSessions.spark.sql(
      " create table if not exists " + Starter.STG_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )

    JobSessions.spark.sql(
      " create table if not exists " + Starter.DWH_PREFIX+tableName + " ( " + colList + " ) " +
        " partitioned by (startday string) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
    )
  }

  def InsertToSTG (df: DataFrame) ={
    df.createOrReplaceTempView("stgAggregationConnectionNode")
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.STG_PREFIX+tableName + " partition (startday) " +
        " select " +
        " nodeid, " +
        " starttime, " +
        " conduration, " +
        " conpercent, " +
        " disconnects, " +
        " enddt, " +
        " endtime, " +
        " startdt, " +
        " starthr, " +
        " unix_timestamp() as ts, " +
        " startday " +
        " from stgAggregationConnectionNode "
    )
  }

  def UpdateDWH ={
    JobSessions.spark.sql("set hive.exec.dynamic.partition=true")
    JobSessions.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    JobSessions.spark.sql(
      " insert into " + Starter.DWH_PREFIX+tableName + " partition (startday) " +
        " select  nodeid,starttime,conduration,conpercent,disconnects," +
        "         enddt,endtime,startdt,starthr,ts,startday " +
        " from " +
        " ( " +
        " select " +
        " nodeid, " +
        " starttime, " +
        " conduration, " +
        " conpercent, " +
        " disconnects, " +
        " enddt, " +
        " endtime, " +
        " startdt, " +
        " starthr, " +
        " max(ts) over (partition by nodeid, starttime) as maxts, " +
        " ts, " +
        " startday " +
        " from " + Starter.STG_PREFIX+tableName +
        " where startday in ('" + Starter.filterPartitionYesterday + "', '" + Starter.filterPartitionToday + "') " +
        " ) a " +
        " where a.ts = a.maxts "
    )
  }

  def GetPartitions(from: String): DataFrame = {
    import JobSessions.spark.implicits._
    import scala.collection.convert.wrapAll._
    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

    val stmt =
      " select distinct nodeid " +
        " from " + Starter.inp_cassKeyspace + "." + tableName
    //" where  starttime >= " + from + " " +
    //" allow  filtering "

    val nodelist = cassSession.execute(stmt).all().toList

    val df = nodelist.map(r => new PartitionList(
      r.getString(0)
    )).toDF().distinct()

    cassSession.close()
    return df
  }

}
