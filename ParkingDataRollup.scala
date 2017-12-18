
package com.sensity.rollup

/**
Created by iozsaracoglu on 01/06/2017.
JIRA Ticket: NSN-3515 ilker
  Parking data rollup process on Spark.
  Input data:
    Cassandra tables, parking_spot_historic (or dwh_parking_spot_historic if DWH Source is not OFF),
                      parking_zone_status and parking_spot_current

  Config data:
    Cassandra table, aggregation_control
  Output data:
    Cassandra tables, aggregation_parking_spot, aggregation_parking_zone, aggregation_parking_group, aggregation_parking_site
    If data warehousing population is ON, then dwh_parking_spot table of DWH
  Aggregated elements: Turnovers, occupy time, occupy percentage
  This process is developed to pre-populate rolled-up parking values for all incoming data in pre-defined time intervals (time buckets). Currently we are
  supporting 15min time intervals as the lowest level aggregation unit; resolution is 15 mins. This is set in "Starter.bucketSize" in the code. If this
  value is changed to a lower value, a re-processing of all historic data will be needed. Higher level time buckets (1hr, day, etc) can be computed based
  on the existing lower level aggregations. This can achieved by running futher "groupBy" operations.
  This process is intended to be running periodically in a cron job or any other job scheduler. Note that, currently only a single instance of this process
  should be running. This is not a scalability concern as Spark performs operations in parallel and scales naturally by its internal partitiong of data.
  But, if for any reason we decide to run multiple instances of this process, this can be achieved by dividing data for "siteid" and maintaining seperate
  high water mark records in "aggregation_control" table.
  Job Arguments:
    Cassandra IP       : Required.
    Run Mode           : Required. [PROD_YARN | PROD_LOCAL | TEST_YARN | TEST_LOCAL]
    Rollup Start       : Optional. Default is High-water-mark from the previous run. Must be in yyyy-MM-dd HH:mm format.
    Rollup End         : Optional. Default is current time (now) when the job is sterted.
    Site Event Limit   : Optional. Default is 0 for all events. [0 | a non-zero positive long value]
    Site               : Optional. Default is ALL for all sites. [ALL | a siteid]
    DWH Source         : Optional. Default is OFF. [dwh_schema | OFF]
    Spark Cleaner TTL  : Optional. Default is 3600.
    Cassandra Keyspace : Optional. Default is "farallones".
    Cassandra port     : Optional. Default is 9042.
  */

import java.nio.file.Path

import org.apache.spark.{SparkConf, TaskContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.joda.time._
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types._
import org.json4s.jackson.JsonMethods._

object Starter {

  case class ParkingSpotHistoric(parkingspotid: String, since: Long, occupancy: Boolean, parkingzoneid: String )
  case class ParkingZoneStatus(siteid: String, parkinggroupid: String, parkingzoneid: String, ptype: String, config: String)
  case class ParkingOccupancyStatus(siteid: String, starttime: Long, parkingspotid: String, occupancy: Boolean)
  case class AggregationControl(typ: String, key: String, value: String)
  case class ParkingZoneGroup(parkingzoneid: String, parkinggroupid: String)
  case class AggregationParkingSpot(record_type:String, siteid:String,parkingspotid:String, starttime:Long, groupid:String, zoneid:String,
                                    parkingtype:String, endtime:Long, occduration:Long, turnovers:Int, occpercent:Double,
                                    startdt:String, enddt:String, starthr:String, startday:String, ts:Long)
  case class ParkingSpotZone(parkingspotid: String, parkingzoneid: String, demarcated: Boolean, active: Boolean )

  val StructParkingSpot = StructType(
      StructField("record_type", StringType, false) ::
      StructField("siteid", StringType, false) ::
      StructField("parkingspotid", StringType, false) ::
      StructField("starttime", LongType, false) ::
      StructField("groupid", StringType, false) ::
      StructField("zoneid", StringType, false) ::
      StructField("parkingtype", StringType, false) ::
      StructField("endtime", LongType, false) ::
      StructField("occduration", LongType, false) ::
      StructField("turnovers", IntegerType, false) ::
      StructField("occpercent", DoubleType, false) ::
      StructField("startdt", StringType, false) ::
      StructField("enddt", StringType, false) ::
      StructField("starthr", StringType, false) ::
      StructField("startday", StringType, false) ::
      StructField("ts", LongType, false) ::
      Nil)
/*
  val StructParkingOcc = StructType(
    StructField("siteid", StringType, false) ::
      StructField("starttime", LongType, false) ::
      StructField("parkingspotid", StringType, false) ::
      StructField("occupancy", BooleanType, false) ::
      Nil)
*/
  val SparkAppName = "parking-rollup"

  // 15 min bucket size (in microseconds) as the lowest level time window for rollups - this is the aggregation resolution
  val bucketSize = 1000000*60*15;

  /*
  Run parameters with default values are listed below. They should be passed to this application during the spark-submit step initiated by the
  scheduled job.
  */
  var inp_cassIP = "52.40.52.243" // Use QA Cassandra instance as default. Must be passed for production runs.

  var inp_runMode = "TEST_LOCAL"  // Run in local[*] mode as default. Must be passed for production runs.
  // Accepted values: TEST_LOCAL, TEST_YARN, PROD_YARN, PROD_LOCAL.

  var inp_startFromDT = "NONE" // In yyyy-MM-dd HH:mm format. If passed, will start the rollup at the most recent top of the hour of this value. If
  // not passed, HWM will be used. Parameter should be used for testing only.
  var inp_processUntilDT = "NONE" // In yyyy-MM-dd HH:mm format. If passed, will end the rollup at the most recent top of the hour of this value. If
  // not passed, "now" will be used. Parameter should be used for testing only.

  var inp_eventRecordLimit = "0" // If passed a non-zero value, will limit the number of event records per site to be processed. Default is 0 for all. Should be used for testing only.

  var inp_site = "ALL" // If passed, will process records for a single site. Should be used for testing  only.

  var inp_dwh = "OFF" // Data warehouse input source will be used. This source is populated by the RefeshDWH job. Defaut is OFF, which will use cassandra as input source.

  var inp_sparkCleanerTTL = "3600"
  var inp_cassKeyspace = "farallones"
  var inp_cassPort = "9042"

  var HWM_LIMIT_MINS = "na"

  /*
  TODO:
  This process should run in a job scheduler (cronjob, etc.) in regular intervals, like every hour, or more frequently depending on rollup speed
  and processing window size. Processing window is a moving window of time which is based on a high-water-mark recording between the runs.
  */

  val rootLogger = Logger.getRootLogger
  val sparkLogging = Logger.getLogger("org.apache.spark")
  val cassandraLogging = Logger.getLogger("com.datastax")

  // Rollup variables
  var dtLast = DateTime.now(DateTimeZone.UTC)
  var initialBucketStartTime = 0L
  val timeEnd = DateTime.now(DateTimeZone.UTC)
  var rollupUntil = timeEnd.withSecondOfMinute(0)
  var eventLimit = 1000000000 // insanity limit

  val CLEANUP_DAYS = 2 // Truncate partitions in staging if older than this setting
  val STG_PREFIX = "stg_"
  val DWH_PREFIX = "fs_"
  val SV_PREFIX = "sv_"
  val filterPartitionToday = DateTime.now(DateTimeZone.UTC).toString("yyyy-MM-dd")
  val filterPartitionYesterday = DateTime.now(DateTimeZone.UTC).minusDays(1).toString("yyyy-MM-dd")

  def main(args: Array[String]) {

    receiveParameters(args)

    InitCassandra.apply()

    if (inp_startFromDT == "NONE") {
      dtLast = HighWaterMark.get()
      if ( Starter.inp_dwh == "OFF" && HWM_LIMIT_MINS != "na" && dtLast.plusMinutes(HWM_LIMIT_MINS.toInt).isBeforeNow) {
        dtLast = timeEnd.minusMinutes(HWM_LIMIT_MINS.toInt)
      }

      // was 4 Get the most recent rollup run time - 1*bucket size
      if (Starter.inp_dwh == "OFF")
        dtLast = dtLast.minusSeconds((Starter.bucketSize / 1000000) * 1)
      else
        dtLast = dtLast.minusSeconds((Starter.bucketSize / 1000000) * 6)
    }
    else
      dtLast = getDateFrom(inp_startFromDT) // Rollup start time passed as input

    initialBucketStartTime = dtLast.withMinuteOfHour(0).getMillis * 1000
    if (initialBucketStartTime + bucketSize < dtLast.getMillis*1000)
      initialBucketStartTime = initialBucketStartTime+bucketSize
    if (initialBucketStartTime + bucketSize < dtLast.getMillis*1000)
      initialBucketStartTime = initialBucketStartTime+bucketSize
    if (initialBucketStartTime + bucketSize < dtLast.getMillis*1000)
      initialBucketStartTime = initialBucketStartTime+bucketSize

    if (inp_processUntilDT == "NONE")
      rollupUntil = timeEnd.minusSeconds((Starter.bucketSize/1000000)*1) // now - 1*bucket size
    else
      rollupUntil = getDateFrom(inp_processUntilDT) // Rollup end time passed as input

    rootLogger.info("Cassandra Keyspace : " + inp_cassKeyspace)
    rootLogger.info("Cassandra IP       : " + inp_cassIP)
    rootLogger.info("Run Mode           : " + inp_runMode)
    rootLogger.info("Rollup Start       : " + inp_startFromDT)
    rootLogger.info("Rollup End         : " + inp_processUntilDT)
    rootLogger.info("Site Event Limit   : " + inp_eventRecordLimit)
    rootLogger.info("Parking Site       : " + inp_site)
    rootLogger.info("DWH Source         : " + inp_dwh)
    rootLogger.info("Spark Cleaner TTL  : " + inp_sparkCleanerTTL)
    rootLogger.info("Cassandra port     : " + inp_cassPort)

    // Processing window: dtLast - rollupUntil.
    rootLogger.info("Rollup started for data after " + dtLast.toString("yyyy-MM-dd HH:mm") +
      " until " + rollupUntil.toString("yyyy-MM-dd HH:mm") )

    // Spot level aggregations for the time bucket size (resolution)
    ProcessSpots.run(initialBucketStartTime, dtLast, rollupUntil)

    /*
    Write back to HWM table.
    */

    HighWaterMark.write()

    JobSessions.sc.stop()

    rootLogger.info("Rollup Completed in " + ((DateTime.now(DateTimeZone.UTC).getMillis - timeEnd.getMillis)/1000).toInt.toString + " seconds.")
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

  def receiveParameters(args: Array[String]) = {
    // Process Job Arguments
    if (args.length>0) inp_cassKeyspace = args(0)
    if (args.length>1) inp_cassIP = args(1)
    if (args.length>2) inp_runMode = args(2)

    if (args.length>3) inp_startFromDT = args(3)
    if (args.length>4) inp_processUntilDT = args(4)

    if (args.length>5) inp_eventRecordLimit = args(5)
    if (args.length>6) inp_site = args(6)
    if (args.length>7) inp_dwh = args(7)

    if (args.length>8) inp_sparkCleanerTTL = args(8)
    if (args.length>9) inp_cassPort = args(9)

    System.setProperty("runMode",inp_runMode)

    if (inp_runMode == "TEST_LOCAL" || inp_runMode == "TEST_YARN" ) {
      rootLogger.setLevel(Level.INFO)
      sparkLogging.setLevel(Level.ERROR)
      cassandraLogging.setLevel(Level.ERROR)
    }

    if (inp_eventRecordLimit != "0") eventLimit = inp_eventRecordLimit.toInt

    var pVal = "na"
    pVal = Parameters.get("hwmlimitmins")
    if (pVal != "na") HWM_LIMIT_MINS = pVal
  }

  // Conversion from yyyyMMddHHmm to DateTime.
  def getDateFrom(inp: String): DateTime = {
    if (inp == "na") return new DateTime(1970,1,1,0,0,DateTimeZone.UTC) // Epoch start
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



object ProcessAggregationParkingSpot {

  val tableName = "aggregation_parking_spot"
  val rootLogger = Logger.getRootLogger

  def run(df:DataFrame) = {

    rootLogger.info("Updating DWH (aggregation_parking_spot)...")

    CreateDWHTable
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


object InitCassandra {

  def apply() = {

    val cassSession = CassandraConnector(JobSessions.sc.getConf).openSession()

    val stmt =
      " insert into " +  Starter.inp_cassKeyspace + ".aggregation_control(type, key, val) " +
      " values ('parking','last_agg_min','" + DateTime.now(DateTimeZone.UTC).minusMinutes(60).toString("yyyy-MM-dd HH:mm") + "') if not exists "
    //" values ('parking','last_agg_min','" + DateTime.now(DateTimeZone.UTC).withHourOfDay(0).withMinuteOfHour(0).toString("yyyy-MM-dd HH:mm") + "') if not exists "

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
    .set("spark.cassandra.output.consistency.level", "ONE")
    .set("spark.cleaner.ttl", Starter.inp_sparkCleanerTTL)
    .setAppName(Starter.SparkAppName)

  if (Starter.inp_runMode == "TEST_LOCAL" || Starter.inp_runMode == "PROD_LOCAL" ) conf.setMaster("local[*]")

  val spark = SparkSession
    .builder()
    .config(conf)
    //.config("spark.sql.warehouse.dir","/tmp/spark-warehouse")
    .enableHiveSupport()
    .getOrCreate()

  val sc = spark.sparkContext

  if (Starter.inp_dwh != "OFF") {
    JobSessions.spark.catalog.setCurrentDatabase(Starter.inp_dwh)
    //JobSessions.spark.sql("create database if not exists " + Starter.inp_dwh.substring(2))
    //JobSessions.spark.catalog.setCurrentDatabase(Starter.inp_dwh.substring(2))
    //if (Starter.inp_dwh.substring(0,1) != "A") JobSessions.spark.sql("drop table if exists job_parking_data")
  }

  Logger.getRootLogger.info("Connected to Sessions.")
}


object HighWaterMark {
  /*
  High water mark for the rollup runs are persisted in a Cassandra table.
  */

  def get(): DateTime = {

    val cass_table = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "aggregation_control")
    val c1 = cass_table.where(
      "       type = 'parking' " +
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

    if (Starter.inp_eventRecordLimit == "0" && Starter.inp_startFromDT == "NONE"
      && Starter.inp_processUntilDT == "NONE") {
      /*
      Writes high-water-mark for the next instance of roll-up.
      */
      Starter.rootLogger.info("Adjusting HWM...")
      val collection = JobSessions.sc.parallelize(Seq((
        "parking", "last_agg_min", Starter.rollupUntil.toString("yyyy-MM-dd HH:mm"))
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

object GetSitesData {
  /*
  Generates new incoming data sets for the site(s)
  */

  var DFjoinedData = JobSessions.spark.sql("create temporary view x as select 'a' as fl ")
  var listCapacity = ListBuffer[Row]()

  def apply(from:String, until:String): DataFrame = {

    import JobSessions.spark.implicits._

    Starter.rootLogger.info("from         : " + from )
    Starter.rootLogger.info("until        : " + until )
    Starter.rootLogger.info("bucket start : " + Starter.initialBucketStartTime )

    val cass_table_zg = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "parking_zone_group")
      .select("parkingzoneid","parkinggroupid")
    val rddzg = cass_table_zg.map(r => new Starter.ParkingZoneGroup(
      r.getString("parkingzoneid"),
      r.getString("parkinggroupid")
    ))
    val DFparkingZoneGroups = rddzg.toDF()
    DFparkingZoneGroups.createOrReplaceTempView("DFparkingZoneGroups")

    val cass_table = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "parking_zone_status")
      .select("siteid", "parkinggroupid", "parkingzoneid", "type", "config")
    val rdd = cass_table.map(r => new Starter.ParkingZoneStatus(
      r.getString("siteid"),
      r.getString("parkinggroupid"),
      r.getString("parkingzoneid"),
      r.getString("type"),
      r.get[Option[String]]("config").getOrElse("na")
    ))
    val DFparkingZones = rdd.toDF()


    val cass_table_cur = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "parking_spot_current")
      .select("parkingspotid","parkingzoneid","demarcated","active")
    val rddz = cass_table_cur.map(r => new Starter.ParkingSpotZone(
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
    dfZoneCapacity.createOrReplaceTempView("DFparkingZones")

    //JobSessions.spark.sql("select * from DFparkingZones").foreach(r=>{
    //  println(r)
    //})

    var siteStmt = "na"
    if (Starter.inp_site == "ALL") siteStmt = "select distinct siteid from DFparkingZones order by siteid"
    else siteStmt = "select distinct siteid from DFparkingZones where siteid = '" + Starter.inp_site + "' order by siteid"

    val DFsites = JobSessions.spark.sql(siteStmt)

    DFsites.createOrReplaceTempView("DFsites")

    Starter.rootLogger.info("Number of sites to search for parking events: " + DFsites.count() )

    val DFsiteGroup = Starter.pivotGroup(DFsites, 10, "String")

    import JobSessions.spark.implicits._
    JobSessions.spark.emptyDataset[Starter.ParkingSpotHistoric].toDF().createOrReplaceTempView("allsitesdata")
    JobSessions.spark.emptyDataset[Starter.ParkingOccupancyStatus].toDF().createOrReplaceTempView("allocc")
    JobSessions.spark.emptyDataset[Starter.ParkingOccupancyStatus].toDF().createOrReplaceTempView("alloccHist")

    DFsiteGroup.collect().foreach(r => {

      import JobSessions.spark.implicits._

      val siteidGrp = r.getString(0)
      var df = JobSessions.spark.emptyDataset[Starter.ParkingSpotHistoric].toDF()

      if (Starter.inp_dwh == "OFF") {
        val cass_table = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "parking_spot_historic")
        val c1 = cass_table.where(
            "     siteid in (" + siteidGrp + ") " +
            " and since >= " + (from.toLong - Starter.bucketSize) + " " +
            " and since < " + until
        ).select("parkingspotid", "since", "occupancy", "parkingzoneid")
        val c2 = c1.limit(Starter.eventLimit.toLong)

        val rdd = c2.map(r => new Starter.ParkingSpotHistoric(
          r.get[Option[String]]("parkingspotid").getOrElse("na"),
          r.getLong("since"),
          //r.getBoolean("occupancy"),
          r.get[Option[Boolean]]("occupancy").getOrElse(false),
          //r.getString("parkingzoneid")
          r.get[Option[String]]("parkingzoneid").getOrElse("na")
        ))

        df = rdd.toDF()
      }
      else {
        val stmt =
          " select parkingspotid, since, occupancy, parkingzoneid " +
          " from   fs_parking_spot_historic " +
          " where  startday >= " + " cast(date_add(from_unixtime(cast(" + from + "/1000000 as bigint)),-1) as string) " +
          " and    startday <= " + " cast(date_add(from_unixtime(cast(" + until + "/1000000 as bigint)),+1) as string) " +
          " and    siteid in (" + siteidGrp + ") " +
          " and    since >= " + (from.toLong - Starter.bucketSize) + " " +
          " and    since < " + until

        //Starter.rootLogger.info(stmt)

        df = JobSessions.spark.sql(stmt)
      }

      val newdf = df.union(JobSessions.spark.sql("select * from allsitesdata"))
      newdf.createOrReplaceTempView("allsitesdata")

      val cass_table_occ = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "aggregation_parking_status")
      val c1occ = cass_table_occ.where(
        "     siteid in (" + siteidGrp + ") " +
        " and starttime  = " + (Starter.initialBucketStartTime - Starter.bucketSize).toString
      )

      val rddocc = c1occ.map(r => new Starter.ParkingOccupancyStatus(
        r.getString("siteid"),
        r.getLong("starttime"),
        r.getString("parkingspotid"),
        r.getBoolean("occupancy")
      ))

      val dfocc = rddocc.toDF()

      val newdfocc = dfocc.union(JobSessions.spark.sql("select * from allocc"))
      newdfocc.createOrReplaceTempView("allocc")
    })

    val hocc = JobSessions.spark.sql("select * from allocc where occupancy = true")
    hocc.createOrReplaceTempView("hocc")

    val DFoccHist =
      JobSessions.spark.sql(" select o.parkingspotid, o.starttime + " + Starter.bucketSize.toString + " as since, o.occupancy, z.parkingzoneid " +
        " from   hocc o " +
        " JOIN   DFparkingSpotZones z ON z.parkingspotid = o.parkingspotid " +
        " where  o.parkingspotid not in (select parkingspotid from allsitesdata) " +
        " and    (z.demarcated = true or o.occupancy = true) "
      )

    val DFalldata = JobSessions.spark.sql("select * from allsitesdata")
      .union(DFoccHist)
      .repartition($"parkingspotid")
      .sortWithinPartitions("since")

    DFalldata.createOrReplaceTempView("DFallData")

    DFjoinedData = JobSessions.spark.sql(
        " select a.parkingspotid, a.since, a.occupancy, a.parkingzoneid, z.siteid, " +
        " case when zg.parkinggroupid is null then 'na' else zg.parkinggroupid end as parkinggroupid, " +
        " z.ptype, " +
        " max(a.since) over (partition by a.parkingspotid) as maxtime, " +
        " CASE WHEN o.occupancy is NULL THEN false ELSE o.occupancy END as defocc " +
        " from DFallData a " +
        " JOIN DFparkingZones z ON a.parkingzoneid = z.parkingzoneid " +
        " LEFT OUTER JOIN hocc o ON a.parkingspotid = o.parkingspotid " +
        " LEFT OUTER JOIN DFparkingZoneGroups zg ON a.parkingzoneid = zg.parkingzoneid "
    )

    Starter.rootLogger.info("Number of all events found: " + DFjoinedData.count().toString)

    DFjoinedData.createOrReplaceTempView("DFjoinedData")
    return DFjoinedData
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
      " select z.siteid as siteid, z.parkinggroupid as parkinggroupid, z.parkingzoneid as parkingzoneid, z.ptype as ptype, " +
        "        c.capacity as capacity " +
        " from   DFparkingZones z " +
        " LEFT   OUTER JOIN capacity_demarcated c ON c.parkingzoneid = z.parkingzoneid " +
        " where  z.ptype = 'Demarcated' "
    )

    JobSessions.spark.sql(" select parkingzoneid, config from DFparkingZones where ptype = 'NonDemarcated' and config != 'na' ")
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
      " select z.siteid as siteid, z.parkinggroupid as parkinggroupid, z.parkingzoneid as parkingzoneid, z.ptype as ptype, " +
        "        c.capacity as capacity " +
        " from   dfParkingZones z " +
        " LEFT   OUTER JOIN capacity_nondemarcated c ON c.parkingzoneid = z.parkingzoneid " +
        " where  z.ptype = 'NonDemarcated' "
    )

    return dfDemarcatedCapacity.union(dfNonDemarcatedCapacity)
  }

}


object ProcessSpots {
  // Parking spot level aggregation for the lowest level time bucket (TimeBucket.bucketSize)

  def processData (iter: Iterator[Row], initialBucketTS:Long, rollupUntilDT:DateTime): Iterator[Row] = {

    // TimeBucket
    val bucketSize = Starter.bucketSize

    /*
    TODO:
    Start from the top of the hour, the most recent top of the hour before HWM. Re-processing some records each time, which will be fine from the
    functionality perspective, but we should consider performance cost vs. dealing with data transmission delays.
    */

    var listBucketSpot = ListBuffer[Row]()

    var bucketSiteid = "na"
    var bucketGroupid = "na"
    var bucketZoneid = "na"
    var bucketType = "na"
    var bucketSpotid = "na"

    // These are just for a more readable date/time values to help data consunption. They are not used in processing.
    var bucketStart = "na" // yyyy-MM-dd HH:mm
    var bucketEnd = "na" // yyyy-MM-dd HH:mm
    var bucketStartHr = "na" // yyyy-MM-dd HH
    var bucketStartDay = "na" // yyyy-MM-dd

    // Time bucket aggregations, accumulated values
    var startTime = 0L // epoch(microsecs)
    var endTime = 0L // epoch(microsecs)
    var occTime = 0L // duration in microsecs
    var turnovers = 0
    var occPercent = 0.0d

    def writeToTables(occ: Boolean) = {
      /*
      Accumulates rolled-up values to be written back to a Cassandra tables for later consumption by APIs or other ad-hoc data analysis applications.
      */
      listBucketSpot += Row("S", bucketSiteid, bucketSpotid, startTime, bucketGroupid,
        bucketZoneid, bucketType, endTime, occTime, turnovers, occPercent, bucketStart, bucketEnd,
        bucketStartHr, bucketStartDay, DateTime.now(DateTimeZone.UTC).getMillis )

      listBucketSpot += Row("O", bucketSiteid, bucketSpotid, startTime, occ.toString,
        "", "", 0L, 0L, 0, 0.0d, "", "",
        "", "", 0L )

      //println(TaskContext.getPartitionId(),bucketSpotid,bucketStart,occPercent)

    }

    def newSpot(initialBucketTS:Long, site: String, group: String, zone: String, typ: String, spot: String) = {
      /*
      New spot detected. Initialize the time bucket.
      */

      bucketSiteid = site
      bucketGroupid = group
      bucketZoneid = zone
      bucketType = typ
      bucketSpotid = spot

      startTime = initialBucketTS //initialBucketStartTime
      endTime = startTime + bucketSize
      occTime = 0L
      turnovers = 0
      occPercent = 0.0d

      bucketStart = (new DateTime(startTime/1000,DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
      bucketEnd = (new DateTime(endTime/1000,DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
      bucketStartHr = bucketStart.substring(0, 13)
      bucketStartDay = bucketStart.substring(0, 10)

    }
    def addTurnover = {
      /*
      Add a turnover to current bucket
      */

      /*
      TODO:
      Currently I am checking false->true occupy transitions. Need guidance to filter out flapping cases perhaps??, and how please.
      */

      turnovers += 1
    }
    def addOccTime(t: Long) = {
      /*
      Add occupy time to current bucket in microsecs.
      */

      /*
      TODO:
      Need guidance to filter out flapping cases.
      */
      occTime += t
      if (occTime>bucketSize) occTime=bucketSize
    }

    def nextBucket(until: DateTime): Boolean = {
      /*
      Move the time bucket forward.
      */

      // Reached processing window-end. Do not go more.
      if (until.getMillis()*1000 < endTime+bucketSize) return false

      // Prepare the new bucket
      startTime += bucketSize
      endTime += bucketSize
      bucketStart = (new DateTime(startTime/1000, DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
      bucketEnd = (new DateTime(endTime/1000, DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
      bucketStartHr = bucketStart.substring(0, 13)
      bucketStartDay = bucketStart.substring(0, 10)
      occTime = 0L
      turnovers = 0
      occPercent = 0.0d

      return true
    }

    def endBucket(occ: Boolean) = {
      /*
      Data for the current bucket is exhausted. Good time to finalize it.
      */

      /*
      TODO:
      Do we really need a double data type for occPercent ??
      */
      occPercent = ((occTime.toDouble / bucketSize.toDouble)*100.0).toDouble
      writeToTables(occ)
    }

    def least(num1: Long, num2: Long): Long = {
      /*
      This is needed to handle partially/fully empty buckets. I actually do not need to worry about negative cases. I thought I needed it for some
      hypothetical cases, which I did not really see yet. Not sure I am imagining things now.
      */

      if (num1<0) return num2
      else if (num2<0) return num1
      else if ((num1<0) && (num2<0)) return 0L
      else if (num1>num2) return num2
      else return num1
    }
    // end TimeBucket

    var listBucket = ListBuffer[Row]()

    var siteid = "na"
    var groupid = "na"
    var zoneid = "na"
    var ptype = "na"
    var spotid = "na"

    var prevSpot = "na"
    var prevOccupancy = false
    var prevSince = 0L

    var since = 0L
    var occupancy = false
    var goodToGo = true
    var maxtime = 0L

    val myList = iter.toList

    listBucketSpot.clear()

    if (myList.size == 0) return listBucketSpot.toIterator // TimeBucket-listBucketSpot.toIterator

    //println( TaskContext.getPartitionId().toString + " inside the map")

    myList.foreach(s => {
      spotid = s.getString(0)
      since = s.getLong(1)

      occupancy = s.getBoolean(2)
      zoneid = s.getString(3)
      siteid = s.getString(4)
      groupid = s.getString(5)
      ptype = s.getString(6)
      maxtime = s.getLong(7)

      // New parkingspotid
      if (prevSpot != spotid) {
        prevOccupancy = s.getBoolean(8)
        prevSince = 0L
        newSpot(initialBucketTS, siteid, groupid, zoneid, ptype, spotid)
        goodToGo = true
      }

      if ((since >= endTime) && goodToGo) { //TimeBucket-endTime
        // Move the time bucket(s) forward to create/end next bucket(s) until reaching to current record's timestamp.
        do {
          if (prevOccupancy) addOccTime(least(bucketSize, endTime - prevSince))
          endBucket(prevOccupancy) // TimeBucket.endBucket(prevOccupancy)
          goodToGo = nextBucket(rollupUntilDT) // TimeBucket.nextBucket(Starter.rollupUntil)
        }
        while ((goodToGo) && since >= endTime) // TimeBucket.endTime
      }

      // If we are still in the processing time window, go for it. If not, records are effectively skipped.
      if (goodToGo) {
        //if ((since >= TimeBucket.startTime) && (since < TimeBucket.endTime)) {
        if ((since >= startTime) && (since < endTime)) {
          // Current record falls into the current time bucket.
          if (occupancy) {
            // Spot is occupied.
            if (!prevOccupancy) addTurnover // TimeBucket.addTurnover  // It was empty, now occupied, so add a turnover

            if (prevOccupancy) {
              // Add occupancy time true->true cases, which we apparently regularly receive. Adding time between two trues.
              addOccTime(least(since - prevSince, since - startTime))
            }
          } // end-occupied
          else {
            // Spot is empty.
            if (prevOccupancy) {
              // it was occupied, now it is empty, so adding occupancy time.
              addOccTime(least(since - prevSince, since - startTime))
            }
          } // end-empty
        }
      }

      if (since == maxtime && spotid != "na") {
        goodToGo = true
        do {
          if (occupancy) addOccTime(least(bucketSize, endTime - since))
          endBucket(occupancy)
          goodToGo = nextBucket(rollupUntilDT)
        }
        while ((goodToGo))
      }

      prevSpot = spotid
      prevSince = since
      prevOccupancy = occupancy
    })

    return listBucketSpot.toIterator
  }


  def run(initialBucketTS:Long, dtLast:DateTime, rollupUntil:DateTime) = {

    import JobSessions.spark.implicits._

    val dfj = GetSitesData.apply(
      (dtLast.getMillis * 1000).toString,
      (rollupUntil.getMillis * 1000).toString
    )
    .repartition($"parkingspotid")
    .sortWithinPartitions("parkingspotid", "since")

    /*
    if (Starter.inp_dwh != "OFF") {
      Starter.rootLogger.info("Populating dwh schema " + Starter.inp_dwh.substring(2) + " with option " + Starter.inp_dwh.substring(0,1))
      dfj.write.mode(SaveMode.Append).saveAsTable("job_parking_data")
    }
    */

    val myResRDD = dfj.rdd.mapPartitions(iter => {
      processData(iter, initialBucketTS, rollupUntil)
    })

    val DFaggall = JobSessions.spark.createDataFrame(myResRDD, Starter.StructParkingSpot).toDF()
    DFaggall.createOrReplaceTempView("aggall")

    val DFaggspot = JobSessions.spark.sql("select siteid,parkingspotid,starttime,groupid,zoneid,parkingtype,endtime,occduration,turnovers,occpercent,startdt,enddt,starthr,startday,ts from aggall where record_type='S' ")
    DFaggspot.createOrReplaceTempView("aggspot")
    Starter.rootLogger.info("Spot level bucket count: " + DFaggspot.count().toString)

    val DFaggocc = JobSessions.spark.sql("select siteid, starttime, parkingspotid, cast(groupid as boolean) as occupancy from aggall where record_type='O' ")

    /*
    Occupancy status for the time buckets
    */
    DFaggocc.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_parking_status", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    /*
    Spot level rollup for the time buckets
    */
    DFaggspot.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_parking_spot", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    ProcessAggregationParkingSpot.run(DFaggspot)

    /*
    Zone level rollup for the time buckets.
    */
    val DFaggzone = JobSessions.spark.sql(
        " select s.zoneid, s.starttime, s.siteid, " +
        "        min(s.groupid) as groupid, " +
        "        min(s.endtime) as endtime, " +
        "        avg(case s.occduration when 0 then null else s.occduration end) as occduration, " +
        "        sum(s.turnovers) as turnovers, " +
        //"        avg(s.occpercent) as occpercent, " +
        "        round(100*sum(s.occduration) / ( case z.capacity when 0 then null else z.capacity end * " + Starter.bucketSize.toString + " ),1) as occpercent, " +
        "        min(s.startdt) as startdt, " +
        "        min(s.enddt) as enddt, " +
        "        min(s.starthr) as starthr, " +
        "        min(s.startday) as startday, " +
        "        max(s.ts) as ts, " +
        "        z.capacity as capacity, " +
        "        sum(case s.occduration when 0 then 0 else 1 end) as filled_cnt, " +
        "        z.ptype as parkingtype " +
        " from   aggspot s " +
        " JOIN   DFparkingZones z ON z.parkingzoneid = s.zoneid " +
        " group  by s.zoneid, s.starttime, s.siteid, z.capacity, z.ptype "
    )
    DFaggzone.createOrReplaceTempView("DFaggzone")

    //DFaggzone.foreach(r=>{
    //  println(r)
    //})

    DFaggzone.select("zoneid","starttime","siteid","groupid","endtime","occduration","turnovers","occpercent","startdt","enddt","starthr","startday","ts").write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_parking_zone", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    /*
    Group level rollup for the time buckets.
    */

    /*
    val DFagggroup = JobSessions.spark.sql(
        " select groupid, starttime, parkingtype, siteid, " +
        "        min(endtime) as endtime, " +
        "        avg(occduration) as occduration, " +
        "        sum(turnovers) as turnovers, " +
        "        avg(occpercent) as occpercent, " +
        "        min(startdt) as startdt, " +
        "        min(enddt) as enddt, " +
        "        min(starthr) as starthr, " +
        "        min(startday) as startday, " +
        "        max(ts) as ts " +
        " from   aggspot " +
        " group  by groupid, starttime, parkingtype, siteid "
    )
    */

    val DFagggroup = JobSessions.spark.sql(
      " select groupid, starttime, parkingtype, siteid, " +
      "        min(endtime) as endtime, " +
      "        round(sum(occduration*filled_cnt)/sum(case filled_cnt when 0 then null else filled_cnt end),0) as occduration, " +
      "        sum(turnovers) as turnovers, " +
      "        round(sum(occpercent*capacity)/sum(case capacity when 0 then null else capacity end),1) as occpercent, " +
      "        min(startdt) as startdt, " +
      "        min(enddt) as enddt, " +
      "        min(starthr) as starthr, " +
      "        min(startday) as startday, " +
      "        max(ts) as ts " +
      " from   DFaggzone " +
      " group  by groupid, starttime, parkingtype, siteid "
    )

    //DFagggroup.foreach(r=>{
    //  println(r)
    //})

    DFagggroup.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_parking_group", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    /*
    Site level rollup for the time buckets.
    */

    /*
    val DFaggsite = JobSessions.spark.sql(
      " select siteid, starttime, parkingtype, " +
        "        min(endtime) as endtime, " +
        "        avg(occduration) as occduration, " +
        "        sum(turnovers) as turnovers, " +
        "        avg(occpercent) as occpercent, " +
        "        min(startdt) as startdt, " +
        "        min(enddt) as enddt, " +
        "        min(starthr) as starthr, " +
        "        min(startday) as startday, " +
        "        max(ts) as ts " +
        " from   aggspot " +
        " group  by siteid, starttime, parkingtype "
    )
    */

    val DFaggsite = JobSessions.spark.sql(
      " select siteid, starttime, parkingtype, " +
      "        min(endtime) as endtime, " +
      "        round(sum(occduration*filled_cnt)/sum(case filled_cnt when 0 then null else filled_cnt end),0) as occduration, " +
      "        sum(turnovers) as turnovers, " +
      "        round(sum(occpercent*capacity)/sum(case capacity when 0 then null else capacity end),1) as occpercent, " +
      "        min(startdt) as startdt, " +
      "        min(enddt) as enddt, " +
      "        min(starthr) as starthr, " +
      "        min(startday) as startday, " +
      "        max(ts) as ts " +
      " from   DFaggzone " +
      " group  by siteid, starttime, parkingtype "
    )

    //DFaggsite.foreach(r=>{
    //  println(r)
    //})

    DFaggsite.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_parking_site", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

  }
}



