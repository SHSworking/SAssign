object MainTest {
  def main(args: Array[String]) : Unit = {
    var simulateFiles : SimulateFiles = null
    var application : ProcessAggregationCount = null
    try {
      println(s"Java ver : ${System.getProperty("java.version")}")
      println(s"Java home : ${System.getProperty("java.home")}")

      //simulate data for testing
      // generating parquet files - File1 (for list of events detected), File 2 (Geo-Loc Ref Table)
      simulateFiles = new SimulateFiles()

      //run the process for aggregated count (rank) of items for each geo-loc
      application = new ProcessAggregationCount(
        "../input/file1event.parquet",
        "../input/file2reftable.parquet",
        "../output/output.parquet",
        2
      )
      println("application run")
    }
    catch {
      case e: UnsupportedOperationException =>
        println(s"UnsupportedOperationException ::: ${e.getMessage}")//caused by Sparks using deprecated java
      case e: Exception =>
        println(s"An error ::: ${e.getMessage}")
    } finally {
      application = null
      simulateFiles = null
      println("application ended")
    }
  }
}
