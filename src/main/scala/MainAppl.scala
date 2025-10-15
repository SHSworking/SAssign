import scala.util.{Failure, Success}

object MainAppl {
  def main(args: Array[String]) : Unit = {
    val settingsfile: String = "settings.config"
    var application : ProcessAggregationCount = null

    try {
      // map input-detection-file1, input-ref-table-file2, output-file, top-n-row defined in settings file
      LoadSettingsConfig.loadAppConfig(settingsfile) match {
        case Success(config) =>
          LoadSettingsConfig.printConfig(config)
          application = new ProcessAggregationCount(
            config.file1,
            config.file2,
            config.outputFile,
            config.topNRow.toInt
          )

        case Failure(e) =>
          println(s"load config file error: ${e.getMessage}")
      }
    }
    catch {
      case e: UnsupportedOperationException =>
        println(s"UnsupportedOperationException ::: ${e.getMessage}")//e.g. if Sparks using deprecated java op
      case e: Exception =>
        println(s"An error ::: ${e.getMessage}")
    } finally {
      application = null
      println("application ended")
    }
  }
}
