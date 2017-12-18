package com.sensity.rollup

/**
JIRA Ticket: NSN-4578

  Traffic data rollup process on Spark.

  Input data:
    Cassandra table, traffic_status

  Config data:
    Cassandra table, aggregation_control

  Output data:
    Cassandra table, aggregation_traffic_events

  Aggregated elements: Event Count, Object Velocity

  This process is developed to pre-populate rolled-up traffic event values for all incoming data in pre-defined time intervals (time buckets). Currently we are
  supporting 15min time intervals as the lowest level aggregation unit; resolution is 15 mins. This is set in "Starter.bucketSize" in the code. If this
  value is changed to a lower value, a re-processing of all historic data will be needed. Higher level time buckets for 1hr and 1day are also computed.

  This process is intended to be running periodically in a cron job or any other job scheduler. Note that, currently only a single instance of this process
  should be running. This is not a scalability concern as Spark performs operations in parallel and scales naturally by its internal partitiong of nodeid.

  Job Arguments:
    Cassandra IP       : Required.
    Run Mode           : Required. [PROD_YARN | PROD_LOCAL | TEST_YARN | TEST_LOCAL]
    Rollup Start       : Optional. Default is High-water-mark from the previous run. Must be in yyyy-MM-dd HH:mm format.
    Rollup End         : Optional. Default is current time (now) when the job is sterted.
    Node Event Limit   : Optional. Default is 0 for all events. [0 | a non-zero positive long value]
    Node               : Optional. Default is ALL for all nodes. [ALL | a nodeid]
    DWH Population     : Optional. Default is ON. [ON | OFF]
    Spark Cleaner TTL  : Optional. Default is 3600.
    Cassandra Keyspace : Optional. Default is "farallones".
    Cassandra port     : Optional. Default is 9042.

  */

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

  case class TrafficStatus(nodeid: String, time: Long, active: Boolean, channel: Int, count: Long, data: String, eventname: String, eventtype: String, orgid: String, siteid: String, tags: List[String], trafficdetectioneventid: String, updated: String)
  case class AggregationControl(typ: String, key: String, value: String)
  case class TrafficEvents(nodeid:String, starttime:Long, eventname:String, eventtype:String, eventid:String, data: String, eventtime:Long, objectclass:String, objectid:String, objectvelocity:Double, dwell: Double,
                           siteid:String, endtime:Long, startdt:String, enddt:String, starthr:String, startday:String, ts:Long)

  val StructTrafficNode = StructType(
      StructField("record_type", StringType, false) ::
      StructField("nodeid", StringType, false) ::
      StructField("starttime", LongType, false) ::
      StructField("eventname", StringType, false) ::
      StructField("eventtype", StringType, false) ::
      StructField("eventid", StringType, false) ::
      StructField("data", StringType, false) ::
      StructField("eventtime", LongType, false) ::
      StructField("objectclass", StringType, true) ::
      StructField("objectid", StringType, true) ::
      StructField("objectvelocity", DoubleType, true) ::
      StructField("dwell", DoubleType, true) ::
      StructField("siteid", StringType, false) ::
      StructField("endtime", LongType, false) ::
      StructField("startdt", StringType, false) ::
      StructField("enddt", StringType, false) ::
      StructField("starthr", StringType, false) ::
      StructField("startday", StringType, false) ::
      StructField("ts", LongType, false) ::
  Nil)

  val SparkAppName = "traffic-rollup"

  // 15 min bucket size (in microseconds) as the lowest level time window for rollups - this is the aggregation resolution
  val bucketSize = 1000000*60*15;

  /*
  Run parameters with default values are listed below. They should be passed to this application during the spark-submit step initiated by the
  scheduled job.
  */
  var inp_cassIP = "52.40.52.243" // Use QA Cassandra instance as default. Must be passed for production runs.

  var inp_runMode = "TEST_LOCAL"  // Run in local[*] mode as default. Must be passed for production runs.
  // Accepted values: TEST_LOCAL, TEST_YARN, PROD_YARN, PROD_LOCAL.

  var inp_rollupMode = "HR" // Rollup start from the start of most recent hour.
  // Accepted values: HR, DAY

  var inp_startFromDT = "NONE" // In yyyy-MM-dd HH:mm format. If passed, will start the rollup at the most recent top of the hour of this value. If
  // not passed, HWM will be used. Parameter should be used for testing only.
  var inp_processUntilDT = "NONE" // In yyyy-MM-dd HH:mm format. If passed, will end the rollup at the most recent top of the hour of this value. If
  // not passed, "now" will be used. Parameter should be used for testing only.

  var inp_eventRecordLimit = "0" // If passed a non-zero value, will limit the number of event records per node to be processed. Default is 0 for all. Should be used for testing only.

  var inp_node = "ALL" // If passed, will process records for a single node. Should be used for testing  only.

  var inp_dwh = "OFF" // Data warehousing records population schema (A_schema for Append or O_schema for Owerride) or OFF for low level data. Defaut is OFF.

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
  val timeEnd = DateTime.now(DateTimeZone.UTC)
  var rollupUntil = timeEnd.withMinuteOfHour(0)
  var eventLimit = 1000000000 // insanity limit
  var minDwellTime = 2000000.0d // 2 secs in microseconds

  def main(args: Array[String]) {

    receiveParameters(args)

    InitCassandra.apply()

    if (inp_startFromDT == "NONE") {
      dtLast = HighWaterMark.get() // Get the most recent rollup run time
      if (HWM_LIMIT_MINS != "na" && dtLast.plusMinutes(HWM_LIMIT_MINS.toInt).isBeforeNow) {
        dtLast = timeEnd.minusMinutes(HWM_LIMIT_MINS.toInt)
      }
    }
    else
      dtLast = getDateFrom(inp_startFromDT) // Rollup start time passed as input

    if (inp_rollupMode == "DAY") {
      // Always start from the 00:00 hour prev day so that we will have complete day for 1day aggregations
      dtLast = dtLast.withMinuteOfHour(0).withHourOfDay(0).minusDays(1)
    } else { // Must be HR mode
      dtLast = dtLast.withMinuteOfHour(0)
    }

    if (inp_processUntilDT == "NONE")
      rollupUntil = timeEnd // Rollup - until now
    else
      rollupUntil = getDateFrom(inp_processUntilDT) // Rollup end time passed as input

    rootLogger.info("Cassandra Keyspace : " + inp_cassKeyspace)
    rootLogger.info("Cassandra IP       : " + inp_cassIP)
    rootLogger.info("Run Mode           : " + inp_runMode)
    rootLogger.info("Rollup Mode        : " + inp_rollupMode)
    rootLogger.info("Rollup Start       : " + inp_startFromDT)
    rootLogger.info("Rollup End         : " + inp_processUntilDT)
    rootLogger.info("Node Event Limit   : " + inp_eventRecordLimit)
    rootLogger.info("Node               : " + inp_node)
    rootLogger.info("DWH Schema         : " + inp_dwh)
    rootLogger.info("Spark Cleaner TTL  : " + inp_sparkCleanerTTL)
    rootLogger.info("Cassandra port     : " + inp_cassPort)

    // Processing window: dtLast - rollupUntil.
    rootLogger.info("Rollup started for data after " + dtLast.toString("yyyy-MM-dd HH:mm") +
      " until " + rollupUntil.toString("yyyy-MM-dd HH:mm") )

    // Prepare an empty dataframe for accumulating node level aggregations
    import JobSessions.spark.implicits._
    JobSessions.spark.emptyDataset[Starter.TrafficEvents].toDF().createOrReplaceTempView("aTSdata")

    // Spot level aggregations for the time bucket size (resolution)
    ProcessNodes.run

    /*
    Write back to HWM table.
    */
    HighWaterMark.write()

    JobSessions.sc.stop()

    rootLogger.info("Rollup Completed in " + ((DateTime.now(DateTimeZone.UTC).getMillis - timeEnd.getMillis)/1000).toInt.toString + " seconds.")
  }

  def receiveParameters(args: Array[String]) = {
    // Process Job Arguments
    if (args.length>0) inp_cassKeyspace = args(0)
    if (args.length>1) inp_cassIP = args(1)
    if (args.length>2) inp_runMode = args(2)

    if (args.length>3) inp_rollupMode = args(3)

    if (args.length>4) inp_startFromDT = args(4)
    if (args.length>5) inp_processUntilDT = args(5)

    if (args.length>6) inp_eventRecordLimit = args(6)
    if (args.length>7) inp_node = args(7)
    if (args.length>8) inp_dwh = args(8)

    if (args.length>9) inp_sparkCleanerTTL = args(9)
    if (args.length>10) inp_cassPort = args(10)

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
        " values ('traffic','last_agg_min','" + DateTime.now(DateTimeZone.UTC).minusMinutes(60).toString("yyyy-MM-dd HH:mm") + "') if not exists "

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
    JobSessions.spark.sql("create database if not exists " + Starter.inp_dwh.substring(2))
    JobSessions.spark.catalog.setCurrentDatabase(Starter.inp_dwh.substring(2))
    if (Starter.inp_dwh.substring(0,1) != "A") JobSessions.spark.sql("drop table if exists job_traffic_events")
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
      "       type = 'traffic' " +
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
    && Starter.inp_processUntilDT == "NONE" && Starter.inp_node == "ALL") {
      /*
      Writes high-water-mark for the next instance of roll-up.
      */
      Logger.getRootLogger.info("Adjusting HWM...")
      val collection = JobSessions.sc.parallelize(Seq((
        "traffic", "last_agg_min", Starter.timeEnd.toString("yyyy-MM-dd HH:mm"))
      ))
      collection.saveToCassandra(Starter.inp_cassKeyspace, "aggregation_control",
        SomeColumns(
          "type", "key", "val")
      )
    }
    else {
      Logger.getRootLogger.info("HWM not changed.")
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
      /*
      +
      " where time >= " + from + " " +
      " allow filtering "
      */

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


object GetNodesData {
  /*
  Generates new incoming data sets for the node(s)
  */

  var DFjoinedData = JobSessions.spark.sql("create temporary view x as select 'a' as fl ")

  def apply(from:String, until:String): DataFrame = {

    GetTrafficNodes.apply(from).createOrReplaceTempView("DFTrafficNodes")

    var nodeStmt = "na"
    if (Starter.inp_node == "ALL") nodeStmt = "select distinct nodeid from DFTrafficNodes order by nodeid"
    else nodeStmt = "select distinct nodeid from DFTrafficNodes where nodeid = '" + Starter.inp_node + "' order by nodeid"

    val DFnodes = JobSessions.spark.sql(nodeStmt)
    val DFnodeGroup = Starter.pivotGroup(DFnodes, 100, "String")

    //Starter.rootLogger.info("Number of nodes to search for traffic events: " + DFnodes.count() )

    import JobSessions.spark.implicits._

    JobSessions.spark.emptyDataset[Starter.TrafficStatus].toDF().repartition($"nodeid").createOrReplaceTempView("allTSdata")

    //JobSessions.spark.emptyDataset[Starter.TrafficStatus].toDF().createOrReplaceTempView("allTSEmpty") // is not used currently

    DFnodeGroup.collect().foreach(r => {

      val nodeidGrp = r.getString(0)
      val cass_table = JobSessions.sc.cassandraTable(Starter.inp_cassKeyspace, "traffic_status")

      val c1 = cass_table.where(
          "        nodeid in (" + nodeidGrp + ") " +
          " and    time >= " + from + " " +
          " and    time <  " + until + " "
      )
      val c2 = c1.limit(Starter.eventLimit)

      val rdd = c2.map(r => new Starter.TrafficStatus(
        r.getString("nodeid"),
        r.getLong("time"),
        //r.getBoolean("active"),
        r.get[Option[Boolean]]("active").getOrElse(true),
        //r.getInt("channel"),
        r.get[Option[Int]]("channel").getOrElse(0),
        //r.getLong("count"),
        r.get[Option[Long]]("count").getOrElse(0L),
        //r.getString("data"),
        r.get[Option[String]]("data").getOrElse("na"),
        r.get[Option[String]]("name").getOrElse("na"),
        r.get[Option[String]]("type").getOrElse("na"), // used to be name
        //r.getString("orgid"),
        r.get[Option[String]]("orgid").getOrElse("na"),
        //r.getString("siteid"),
        r.get[Option[String]]("siteid").getOrElse("na"),
        r.get[List[String]]("tags"),
        //r.getString("trafficdetectioneventid"),
        r.get[Option[String]]("trafficdetectioneventid").getOrElse("na"),
        //r.getString("updated")
        r.get[Option[String]]("updated").getOrElse("na")
      ))
      val df = rdd.toDF().repartition($"nodeid")

      val newdf = df.union(JobSessions.spark.sql("select * from allTSdata"))
      newdf.createOrReplaceTempView("allTSdata")
    })

    val DFalldata = JobSessions.spark.sql(
      " select nodeid,time,data,eventname,eventtype,siteid,trafficdetectioneventid from allTSdata "
    )

    return DFalldata
  }
}


object ProcessNodes {
  // Node level aggregation for the lowest level time bucket (TimeBucket.bucketSize)

  def processData (iter: Iterator[Row]): Iterator[Row] = {

    var listBucket = ListBuffer[Row]()

    var nodeid = "na"
    var time = 0L
    var data = "na"
    var eventname = "na"
    var eventtype = "na"
    var siteid = "na"
    var eventid = "na"
    var oclass = "na"
    var oid = "na"
    var ovelocity = 0.0d
    var dwell = 0.0d

    var starttime = 0L
    var endtime = 0L
    var startdt = "na"
    var enddt = "na"
    var starthr = "na"
    var startday = "na"

    var rowAdded = false
    var ts = DateTime.now(DateTimeZone.UTC).getMillis

    val myList = iter.toList

    myList.foreach(s=> {
      nodeid = s.getString(0)
      time = s.getLong(1)
      data = s.getString(2)
      eventname = s.getString(3)
      eventtype = s.getString(4)
      siteid = s.getString(5)
      eventid = s.getString(6)

      rowAdded = false

      val timeDT = new DateTime(time / 1000, DateTimeZone.UTC)
      starttime = (timeDT.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).getMillis) * 1000
      endtime = starttime + Starter.bucketSize
      while (!(time >= starttime && time < endtime)) {
        starttime += Starter.bucketSize
        endtime += Starter.bucketSize
      }
      startdt = (new DateTime(starttime / 1000, DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
      enddt = (new DateTime(endtime / 1000, DateTimeZone.UTC)).toString("yyyy-MM-dd HH:mm")
      starthr = startdt.substring(0, 13)
      startday = startdt.substring(0, 10)

      if (eventtype == "ObjectDwellEvent") {
        dwell = time.toDouble
      }
      else dwell = 0.0d

      try {
        val x = parse(data)
        val fn2 = (x \ "detected_objects")

        fn2.children.foreach(j => {
          rowAdded = true

          try {
            oclass = compact(render((j \ "class"))).replaceAll("\"", "").toLowerCase()
          }
          catch {
            case ex: Exception => {
              oclass = "na"
            }
          }
          if (oclass.length == 0) oclass = "na"

          try {
            oid = compact(render((j \ "detectedobjectid")))
          }
          catch {
            case ex: Exception => {
              oid = "na"
            }
          }

          try {
            val x = compact(render((j \ "velocity" \ "x"))).toDouble
            val y = compact(render((j \ "velocity" \ "y"))).toDouble
            ovelocity = Math.sqrt(Math.pow(x, 2.0) + Math.pow(y, 2.0))
          }
          catch {
            case ex: Exception => {
              ovelocity = 0.0d
            }
          }

          if (ovelocity == 0.0d) {
            try {
              val x = compact(render((j \ "world_velocity" \ "x"))).toDouble
              val y = compact(render((j \ "world_velocity" \ "y"))).toDouble
              ovelocity = Math.sqrt(Math.pow(x, 2.0) + Math.pow(y, 2.0))
            }
            catch {
              case ex: Exception => {
                ovelocity = 0.0d
              }
            }
          }

          if (eventtype == "ObjectDwellEvent") {
            listBucket +=
              Row("D", nodeid, starttime, eventname, eventtype, eventid, data, time, oclass, oid, ovelocity, dwell, siteid, endtime, startdt, enddt, starthr, startday, ts)
          }
          else {
            listBucket +=
              Row("L", nodeid, starttime, eventname, eventtype, eventid, data, time, oclass, oid, ovelocity, dwell, siteid, endtime, startdt, enddt, starthr, startday, ts)
          }


        })
      }
      catch {
        case ex: Exception => {
          rowAdded = false
        }
      }

      if (!rowAdded) {
        if (eventtype == "ObjectDwellEvent") {
          listBucket += Row("D", nodeid, starttime, eventname, eventtype, eventid, data, time, "na", "na", null, null, siteid, endtime, startdt, enddt, starthr, startday, ts)
        }
        else {
          listBucket += Row("L", nodeid, starttime, eventname, eventtype, eventid, data, time, "na", "na", null, null, siteid, endtime, startdt, enddt, starthr, startday, ts)
        }

      }

    }) // iter


    return listBucket.toIterator
  }

  def run() = {

    import JobSessions.spark.implicits._

    val dfj = GetNodesData.apply(
      (Starter.dtLast.getMillis * 1000).toString,
      (Starter.rollupUntil.getMillis * 1000).toString
    )
    .repartition($"nodeid") // needed because of view usage in GetNodesData. We need eventid really but eventid should normally be reported by a single node
    .sortWithinPartitions("time")

    val myResRDD = dfj.rdd.mapPartitions(iter => {
      processData(iter)
    })

    JobSessions.spark.createDataFrame(myResRDD, Starter.StructTrafficNode).createOrReplaceTempView("myResDF")

    val DFa = JobSessions.spark.sql(
      " select nodeid,starttime,eventtype,eventname,eventid,data,eventtime,objectclass,objectid," +
      "        objectvelocity,dwell,siteid,endtime,startdt,enddt,starthr,startday,ts" +
      " from   myResDF " +
      " where  record_type =  'L' "
    )

    val dfDwellCalculated = JobSessions.spark.sql(
      " select nodeid, starttime, eventtype, min(eventname) as eventname, eventid, min(data) as data, min(eventtime) as eventtime, " +
      "        objectclass, objectid, 0.0 as objectvelocity, (max(dwell)-min(dwell)) + " + Starter.minDwellTime.toString + " as dwell, " +
      "        siteid, endtime, startdt, enddt, starthr, startday, max(ts) as ts " +
      " from   myResDF " +
      " where  record_type = 'D' " +
      " group  by nodeid, starttime, eventtype, eventid, objectclass, objectid, siteid, endtime, startdt, enddt, starthr, startday "
    )

    val dfBoth = DFa.union(dfDwellCalculated).repartition($"siteid", $"eventid")
    dfBoth.createOrReplaceTempView("aTSdata")

    val dfCombo = JobSessions.spark.sql (
      " select nodeid, starttime, 'COMBO' as eventtype, comboevent as eventname, comboevent as eventid, 'na' as data, eventtime, objectclass, objectid, " +
      "        null as objectvelocity, null as dwell, siteid, endtime, startdt, enddt, starthr, startday, ts " +
      " from   ( " +
      "        select e.*, " +
      "               concat(nodeid,'==>',lag(eventname) over (partition by nodeid,objectid order by eventtime),'==>',eventname) as comboevent, " +
      "               lag(eventtime) over (partition by nodeid,objectid order by eventtime) as lagtime " +
      "        from   aTSdata e " +
      "        ) t " +
      " where  t.eventtime - t.lagtime < 60000000 "
    )

    val dfwithCombo = dfBoth.union(dfCombo)
    dfwithCombo.createOrReplaceTempView("aTSdata")

    if (Starter.inp_dwh != "OFF") dfwithCombo.write.mode(SaveMode.Append).saveAsTable("job_traffic_events")


    // Node level aggregations

    val dfAgg = JobSessions.spark.sql(
      " select siteid, eventid, '15min' as aggregation_type, starttime, eventtype as type, objectclass, endtime, startdt, enddt, starthr, startday, " +
      "        min(eventname) as name, " +
      "        max(nodeid) as nodeid, " +
      "        avg(case objectvelocity when 0 then null else objectvelocity end) as avgvelocity, " +
      "        avg(case dwell when 0 then null else dwell end) as avgdwell, " +
      "        percentile_approx(case objectvelocity when 0 then null else objectvelocity end, 0.5) as medianvelocity, " +
      "        percentile_approx(case objectvelocity when 0 then null else objectvelocity end, 0.85) as p85velocity, " +
      "        count(*) as eventcnt, " +
      "        max(ts) as ts " +
      " from   aTSdata " +
      " group  by siteid, eventid, starttime, eventtype, objectclass, endtime, startdt, enddt, starthr, startday "
    )

    dfAgg.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_traffic_events", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()


    val dfAggHr = JobSessions.spark.sql(
      " select siteid, eventid, '1hr' as aggregation_type, eventtype as type, objectclass, starthr, startday, " +
      "        min(eventname) as name, " +
      "        max(nodeid) as nodeid, " +
      "        min(starttime) as starttime, " +
      "        3600000000+unix_timestamp(concat(starthr,':00:00'))*1000000 as endtime, " +
      "        min(startdt) as startdt, " +
      "        max(enddt) as enddt, " +
      "        avg(case objectvelocity when 0 then null else objectvelocity end) as avgvelocity, " +
      "        avg(case dwell when 0 then null else dwell end) as avgdwell, " +
      "        percentile_approx(case objectvelocity when 0 then null else objectvelocity end, 0.5) as medianvelocity, " +
      "        percentile_approx(case objectvelocity when 0 then null else objectvelocity end, 0.85) as p85velocity, " +
      "        count(*) as eventcnt, " +
      "        max(ts) as ts " +
      " from   aTSdata " +
      " group  by siteid, eventid, eventtype, objectclass, starthr, startday "
    )

    dfAggHr.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_traffic_events", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    if (Starter.inp_rollupMode == "DAY") {
      val dfAggDay = JobSessions.spark.sql(
        " select siteid, eventid, '1day' as aggregation_type, eventtype as type, objectclass, startday, " +
          "        min(eventname) as name, " +
          "        max(nodeid) as nodeid, " +
          "        min(starttime) as starttime, " +
          "        86400000000+unix_timestamp(concat(startday,' 00:00:00'))*1000000 as endtime, " +
          "        min(startdt) as startdt, " +
          "        max(enddt) as enddt, " +
          "        min(starthr) as starthr, " +
          "        avg(case objectvelocity when 0 then null else objectvelocity end) as avgvelocity, " +
          "        avg(case dwell when 0 then null else dwell end) as avgdwell, " +
          "        percentile_approx(case objectvelocity when 0 then null else objectvelocity end, 0.5) as medianvelocity, " +
          "        percentile_approx(case objectvelocity when 0 then null else objectvelocity end, 0.85) as p85velocity, " +
          "        count(*) as eventcnt, " +
          "        max(ts) as ts " +
          " from   aTSdata " +
          " group  by siteid, eventid, eventtype, objectclass, startday "
      )

      dfAggDay.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "aggregation_traffic_events", "keyspace" -> Starter.inp_cassKeyspace))
        .mode(SaveMode.Append)
        .save()
    }

    // Site level aggregations

    val dfAggSite = JobSessions.spark.sql(
        " select siteid, '15min' as aggregation_type, starttime, eventtype as type, objectclass, endtime, startdt, enddt, starthr, startday, " +
        "        avg(case objectvelocity when 0 then null else objectvelocity end) as avgvelocity, " +
        "        avg(case dwell when 0 then null else dwell end) as avgdwell, " +
        "        percentile_approx(case objectvelocity when 0 then null else objectvelocity end, 0.5) as medianvelocity, " +
        "        percentile_approx(case objectvelocity when 0 then null else objectvelocity end, 0.85) as p85velocity, " +
        "        count(*) as eventcnt, " +
        "        max(ts) as ts " +
        " from   aTSdata " +
        " group  by siteid, starttime, eventtype, objectclass, endtime, startdt, enddt, starthr, startday "
    )

    dfAggSite.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_traffic_events_site", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    val dfAggHrSite = JobSessions.spark.sql(
        " select siteid, '1hr' as aggregation_type, eventtype as type, objectclass, starthr, startday, " +
        "        min(starttime) as starttime, " +
        "        3600000000+unix_timestamp(concat(starthr,':00:00'))*1000000 as endtime, " +
        "        min(startdt) as startdt, " +
        "        max(enddt) as enddt, " +
        "        avg(case objectvelocity when 0 then null else objectvelocity end) as avgvelocity, " +
        "        avg(case dwell when 0 then null else dwell end) as avgdwell, " +
        "        percentile_approx(case objectvelocity when 0 then null else objectvelocity end, 0.5) as medianvelocity, " +
        "        percentile_approx(case objectvelocity when 0 then null else objectvelocity end, 0.85) as p85velocity, " +
        "        count(*) as eventcnt, " +
        "        max(ts) as ts " +
        " from   aTSdata " +
        " group  by siteid, eventtype, objectclass, starthr, startday "
    )

    dfAggHrSite.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "aggregation_traffic_events_site", "keyspace" -> Starter.inp_cassKeyspace))
      .mode(SaveMode.Append)
      .save()

    if (Starter.inp_rollupMode == "DAY") {
      val dfAggDaySite = JobSessions.spark.sql(
          " select siteid, '1day' as aggregation_type, eventtype as type, objectclass, startday, " +
          "        min(starttime) as starttime, " +
          "        86400000000+unix_timestamp(concat(startday,' 00:00:00'))*1000000 as endtime, " +
          "        min(startdt) as startdt, " +
          "        max(enddt) as enddt, " +
          "        min(starthr) as starthr, " +
          "        avg(case objectvelocity when 0 then null else objectvelocity end) as avgvelocity, " +
          "        avg(case dwell when 0 then null else dwell end) as avgdwell, " +
          "        percentile_approx(case objectvelocity when 0 then null else objectvelocity end, 0.5) as medianvelocity, " +
          "        percentile_approx(case objectvelocity when 0 then null else objectvelocity end, 0.85) as p85velocity, " +
          "        count(*) as eventcnt, " +
          "        max(ts) as ts " +
          " from   aTSdata " +
          " group  by siteid, eventtype, objectclass, startday "
      )

      dfAggDaySite.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "aggregation_traffic_events_site", "keyspace" -> Starter.inp_cassKeyspace))
        .mode(SaveMode.Append)
        .save()
    }

  }
}


