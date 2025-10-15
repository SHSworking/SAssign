import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank}

class ProcessAggregationCount ( inputFile1: String, inputFile2: String, outputFile: String,  num_of_topNRow: Int) {
  var conf : SparkConf = null
  var sparkC : SparkContext = null
  try {
    val lStart: Long = System.currentTimeMillis()
    // for displaying processing time, future expansion for further optimization

    // create SparkConf object
    conf = new SparkConf()
      .setAppName("ProcessAggCountOfItemsByGeoLoc")
      .setMaster("local[*]") // Run Spark locally using all available cores


    // create SparkContext using the SparkConf & the SparkSession
    sparkC = new SparkContext(conf)
    val sparkS = SparkSession.builder().config(sparkC.getConf).getOrCreate()

    val sFilePathDataSet1Event: String = inputFile1
    val sFilePathDataSet2RefTable: String = inputFile2 //kept for future expansion
    val sFilePathDataSetOutput: String = outputFile
    val top_N_rank = num_of_topNRow

    // read file1
    val dfTWr1: DataFrame = sparkS.read.parquet(sFilePathDataSet1Event)

    import sparkS.implicits._ //  .as[] conversion for case class
    val ds1: Dataset[Events] = dfTWr1.as[Events]
    ds1.printSchema()
    println("_________ schema of File 1")
    ds1.show()
    println("_________ data of File 1")


    //Convert the read-datafreme to an RDD[Row]
    val ds1RDD_Rows: RDD[Row] = dfTWr1.rdd
    val file1eventRDD: RDD[Events] = ds1RDD_Rows.map{ row =>
      val geoLocId = row.getAs[Long]("geographical_location_oid")
      val camId = row.getAs[Long]("video_camera_oid")
      val detectionId = row.getAs[Long]("detection_oid")
      val itemName = row.getAs[String]("item_name")
      val timestamp = row.getAs[Long]("timestamp_detected")
      Events(geoLocId,camId,detectionId,itemName,timestamp)
    }.distinct() // remove exact duplicates


    //de-duplication, remove duplicates if same detectionId, but different timesstamp
    // and also cross-camera tracking,
    // i.e. detection algorithm can generate same detection_oid (detectionId) acress camera
    val file1Agg = file1eventRDD.map {
        case Events(geoLocId,camId,detectionId,itemName,timestamp) =>
          ((geoLocId,camId,detectionId,itemName,timestamp), 1) //file1KeyValuePairEventRDD
      }.reduceByKey(_ + _)
      .map {case ((geoLocId,camId,detectionId,itemName,timestamp),_) =>
        ((geoLocId,itemName),1)
      }.reduceByKey(_ + _)
      //convert to dataframe with count
      .map {case ((geoLocId,itemName),count) =>
        (geoLocId,itemName,count) //flatten to 3 flat columns
      }.toDF("geoLocId","itemName","count") //.toDS()

    //sort
    val sortedFile1Agg = file1Agg.orderBy(
      file1Agg.col("geoLocId").asc,
      file1Agg.col("count").desc,// highest count shown first
      file1Agg.col("itemName").asc // for tie-break of same count
    )
    sortedFile1Agg.show()
    println(s"_____________ sorted results, time - ${System.currentTimeMillis()-lStart} ms")

    // window function to keep top N rows
    val windowSpec = Window.partitionBy("geoLocId").orderBy(col("count").desc)

    val rankedFile1Agg = sortedFile1Agg.withColumn("rank", rank().over(windowSpec))
                            // keeping tie-break hence use rank() instead of row_number() above

    val mayFilterRankedFile1Agg =
      if (top_N_rank >= 1) {
        rankedFile1Agg.filter(col("rank") <= top_N_rank)
      } else {  //note: filtering is skipped
        rankedFile1Agg
      }

    mayFilterRankedFile1Agg.show()
    println(s"_____________ filtered rank row results, time - ${System.currentTimeMillis()-lStart} ms")

    val selectedColFile1Agg = mayFilterRankedFile1Agg.select(
      col("geoLocId").alias("geographical_location"), //renaming column-name with alias
      col("rank").cast("String").alias("item_rank"),
      //change from Integer to String, to store as parquet StringType
      //[future expansion] to write varchar.length constraints in delta log, if using delta lake for productions ops

      col("itemName").alias("item_name")
    )
    selectedColFile1Agg.show()
    selectedColFile1Agg.printSchema()
    println(s"_____ wrote above dataset, schema to file ${outputFile} , time - ${System.currentTimeMillis()-lStart} ms")

    //write to output parquet file
    selectedColFile1Agg.write.mode("overwrite").parquet(sFilePathDataSetOutput)


    /* no need to read file2 for this processing
    //// For Future expansion : to read file2 when required e.g. using .join with file1
    val dfTWr2: DataFrame = sparkS.read.parquet(sFilePathDataSet2RefTable)
    import sparkS.implicits._ // for .as[] conversion for case class
    val ds2: Dataset[GeoLoc] = dfTWr2.as[GeoLoc]
    println("schema of File 2")
    ds2.printSchema()
    ds2.show()
     */
    sparkS.stop()
  } catch {
    case e: UnsupportedOperationException =>
      println(s"UnsupportedOperationException ::: ${e.getMessage}")//caused by Sparks using depreated java
    case e: Exception =>
      println(s"An error in ProcessAggregationCount::: ${e.getMessage}")
  } finally {
    //stop the SparkContext
    sparkC.stop()
    sparkC=null
    conf=null
    println (s":::closed sparkcontext")
  }
}
