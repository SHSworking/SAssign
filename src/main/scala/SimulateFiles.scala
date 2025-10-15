import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

//Use generate File1 to SimulateFile1EventsDetected
class SimulateFile1Event {
  var conf : SparkConf = null
  var sparkC : SparkContext = null

  // create SparkConf object
  conf = new SparkConf()
    .setAppName("SimulateFile1EventsDetected")
    .setMaster("local[*]") // Run Spark locally using all available cores



  // create SparkContext using the SparkConf & the SparkSession
  sparkC = new SparkContext(conf)
  val sparkS = SparkSession.builder().config(sparkC.getConf).getOrCreate()

  val sFilePathDataSet1Event: String = s"../input/file1event.parquet" //current dir /Users/shs/IdealProject/SAssign
  val sFilePathDataSet2RefTable: String = s"../input/file2reftable.parquet"
  //val sFilePathDataSetOutput: String = s"../output/output1.parquet" //current dir /Users/shs/IdealProject/SAssign

  // simulate write (to create) the read parquet file 1 for doing the groupBy aggregation
  //   count transformation
  //. Find out how to  return top 10 (N=10),
  //Future expansion to match substring (toupper (BICYCLE)  - load into list from enum
  //   ReduceByKey faster /better performance than GroupBy which shuffles
  val testData1 = Seq (
    Row(123456L, 20001234L, 21234001L, "Bicycle", 1760287784696L),
    Row(123456L, 20001234L, 21234001L, "Bicycle", 1760287784696L), // create duplicate row to test distinct
    Row(123456L, 20001235L, 21234002L, "Thrashbin", 1760287784696L),
    Row(223456L, 20001236L, 21234003L, "Luggage", 1760287784696L),
    Row(223456L, 20001237L, 21234004L, "Thrashbin", 1760287784696L),
    Row(333456L, 20001238L, 21234005L, "Thrashbin", 1760287784696L),
    Row(123456L, 20001234L, 21234011L, "Bicycle", 1760287784696L),
    Row(123456L, 20001235L, 21234012L, "Thrashbin", 1760287784696L),
    Row(223456L, 20001236L, 21234013L, "Luggage", 1760287784696L),
    Row(223456L, 20001237L, 21234014L, "Thrashbin", 1760287784696L),
    Row(333456L, 20001238L, 21234015L, "Thrashbin", 1760287784696L),
    Row(333456L, 20001239L, 21234016L, "Bicycle", 1760287784696L)
  )



  val schemaInputFile1 = StructType (Array(
    StructField("geographical_location_oid", LongType, nullable = true),
    StructField("video_camera_oid", LongType, nullable = true),
    StructField("detection_oid", LongType, nullable = true),
    StructField("item_name", StringType, nullable = true),
    StructField("timestamp_detected", LongType, nullable = true)
  ))

  val testData2 = Seq (
    Row(123456L, "10 Dover Close"),
    Row(223456L, "20 Tiong Bahru Road"),
    Row(333456L, "30 Redhill Drive")
  )
  val schemaInputFile2 = StructType (Array(
    StructField("geographical_location_oid", LongType, nullable = false),
    StructField("geographical_location", StringType, nullable = false)
  ))

  //// simulate write File1 amd 2 so that create read
  val rowRDDW1 = sparkS.sparkContext.parallelize(testData1)
  val rowRDDW2 = sparkS.sparkContext.parallelize(testData2)
  println (s"0000 ")

  val dfw1 = sparkS.createDataFrame(rowRDDW1, schemaInputFile1)
  val dfw2 = sparkS.createDataFrame(rowRDDW2, schemaInputFile2)
  println(s"11111  after sparks.createDF")
  //mode("overwrite")    mode("append")
  dfw1.write.mode("overwrite").option("compression","gzip") //snappy default - create dir, lzo for hadoop or prod
    .parquet(sFilePathDataSet1Event)
  dfw2.write.mode("overwrite").option("compression","gzip") //snappy default - create dir, lzo for hadoop or prod
    .parquet(sFilePathDataSet2RefTable)

  //dfw1.printSchema()
  //sdfw2.printSchema()
  println(s"22222222  after write parquet systemtime ${System.currentTimeMillis()} ms")




  //// read file1
  val dfTWr1: DataFrame = sparkS.read.parquet(sFilePathDataSet1Event)
  println(s"333. test read of coded-parquet write - ${sFilePathDataSet1Event}")
  // dfTWr.show()
  import sparkS.implicits._ // for .as[] conversion for case class
  val ds1: Dataset[VideoSensor] = dfTWr1.as[VideoSensor]
  println("3333333333      schema of File 1")
  ds1.printSchema()
  ds1.show()

  import sparkS.implicits._
  val oneLvlAgg = ds1.groupByKey(_.geographical_location_oid).count()
  oneLvlAgg.show()
  val oneLvlAggA = ds1.groupByKey(_.item_name).count()
  oneLvlAggA.show()

  //reduce by key increased performance

  //Convert the read-datafreme to an RDD[Row]
  val ds1RDD_Rows: RDD[Row] = dfTWr1.rdd
  val file1eventRDD: RDD[VideoSensor] = ds1RDD_Rows.map{ row =>
    val geoLocId = row.getAs[Long]("geographical_location_oid")
    val camId = row.getAs[Long]("video_camera_oid")
    val detectionId = row.getAs[Long]("detection_oid")
    val itemName = row.getAs[String]("item_name")
    val timestamp = row.getAs[Long]("timestamp_detected")
    VideoSensor(geoLocId,camId,detectionId,itemName,timestamp)
  }.distinct() // remove exact duplicates
  //    .map{case VideoSensor(geoLocId,camId,detectionId,itemName,timestamp)=>
  //     ((geoLocId,camId,detectionId,itemName,timestamp),1))


  //de-duplication, remove duplicates if same detectionId, but different timesstamp
  // and also cross-camera tracking, i.e. detection algorithm can generate same detection_oid (detectionId) acress camera
  val file1Agg = file1eventRDD.map {
      case VideoSensor(geoLocId,camId,detectionId,itemName,timestamp) =>
        ((geoLocId,camId,detectionId,itemName,timestamp), 1) //file1KeyValuePairEventRDD
    }.reduceByKey(_ + _)
    .map {case ((geoLocId,camId,detectionId,itemName,timestamp),_) =>
      ((geoLocId,itemName),1)
    }.reduceByKey(_ + _)
    //convert to dataframe with count
    .map {case ((geoLocId,itemName),count) =>
      (geoLocId,itemName,count) //flatten to 3 flat columns
    }.toDF("geoLocId","itemName","count") //.toDS()

  println(s"after reduceByKey increase performance")
  file1Agg.show() // show(3) if show top 3 results

  //sort
  val sortedFile1Agg = file1Agg.orderBy(
    file1Agg.col("geoLocId").asc,
    file1Agg.col("count").desc,// highest count shown first
    file1Agg.col("itemName").asc // for tie-break of same count
  )
  sortedFile1Agg.show()
  println(s"_____________ orderby results")

  /* no need to read file2 for this process
    //// read file2
  val dfTWr2: DataFrame = sparkS.read.parquet(sFilePathDataSet2RefTable)
  println(s"333. test read of coded-parquet write - ${sFilePathDataSet2RefTable}")
  import sparkS.implicits._ // for .as[] conversion for case class
  val ds2: Dataset[GeoLoc] = dfTWr2.as[GeoLoc]
  println("3333333333      schema of File 2")
  ds2.printSchema()
  ds2.show()
   */

  //stop the SparkContext & SparkSession
  sparkS.stop()
  sparkC.stop()
  sparkC=null
  conf=null
  println (s":::closed sparkcontext")

}
