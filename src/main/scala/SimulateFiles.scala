import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

//To generate parquet File1, File2 to simulate Events Detected and Geo-locations ref-tables
class SimulateFiles {
  var conf : SparkConf = null
  var sparkC : SparkContext = null

  // create SparkConf object
  conf = new SparkConf()
    .setAppName("SimulateFile1EventsDetected")
    .setMaster("local[*]") // Run Spark locally using all available cores

  // create SparkContext using the SparkConf & the SparkSession
  sparkC = new SparkContext(conf)
  val sparkS = SparkSession.builder().config(sparkC.getConf).getOrCreate()

  val sFilePathDataSet1Event: String = s"../input/file1event.parquet"
  val sFilePathDataSet2RefTable: String = s"../input/file2reftable.parquet"
  //val sFilePathDataSetOutput: String = s"../output/output1.parquet" //for future expansion use

  val testData1 = Seq (
    Row(123456L, 20001234L, 21234001L, "Bicycle", 1760287784696L),
    Row(123456L, 20001234L, 21234001L, "Bicycle", 1760287784696L), // create duplicate row to test .distinct
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

  val rowRDDW1 = sparkS.sparkContext.parallelize(testData1)
  val rowRDDW2 = sparkS.sparkContext.parallelize(testData2)

  val dfw1 = sparkS.createDataFrame(rowRDDW1, schemaInputFile1)
  val dfw2 = sparkS.createDataFrame(rowRDDW2, schemaInputFile2)

  dfw1.write.mode("overwrite").option("compression","gzip") //snappy default - create dir, lzo for hadoop or prod
    .parquet(sFilePathDataSet1Event)
  dfw2.write.mode("overwrite").option("compression","gzip") //snappy default - create dir, lzo for hadoop or prod
    .parquet(sFilePathDataSet2RefTable)

  //Uncomment to display schemas
  //dfw1.printSchema()
  //sdfw2.printSchema()

  //stop the SparkContext & SparkSession
  sparkS.stop()
  sparkC.stop()
  sparkC=null
  conf=null
}
