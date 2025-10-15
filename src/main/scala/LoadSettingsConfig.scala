import scala.io.Source
import scala.util.Try

object LoadSettingsConfig {
  case class AppConfig(
                        file1: String,
                        file2: String,
                        outputFile: String,
                        topNRow: Int,
                        additionalProps: Map[String, String] = Map.empty
                      )

  def loadConfigFile(filePath: String): Map[String, String] = {
    val source = Source.fromFile(filePath)

    try {
      source.getLines()
        .map(_.trim)
        .filterNot(line => line.isEmpty || line.startsWith("#"))
        .flatMap(parseLine)
        .toMap
    } catch {
      case e: Exception =>
        println(s"Error loading config file: ${e.getMessage}")
        Map.empty[String, String]
    } finally {
      source.close()
    }
  }

  private def parseLine(line: String): Option[(String, String)] = {
    val parts = line.split("=", 2)

    if (parts.length == 2) {
      val key = parts(0).trim
      val value = cleanValue(parts(1).trim)
      Some(key -> value)
    } else {
      println(s"Warning: Skipping invalid line: $line")
      None
    }
  }

  private def cleanValue(value: String): String = {
    value
      .replaceAll("^\"|\"$", "")
      .replaceAll("^'|'$", "")
      .trim
  }

  def parseAppConfig(configMap: Map[String, String]): Try[AppConfig] = Try {
    AppConfig(
      file1 = configMap.getOrElse("FILE1",
        throw new IllegalArgumentException("FILE1 is required")),
      file2 = configMap.getOrElse("FILE2",
        throw new IllegalArgumentException("FILE2 is required")),
      outputFile = configMap.getOrElse("OUTPUT_FILE",
        throw new IllegalArgumentException("OUTPUT_FILE is required")),
      topNRow = configMap.get("TOP_N_ROW")
        .map(_.toInt)
        .getOrElse(10),
      additionalProps = configMap - ("FILE1", "FILE2", "OUTPUT_FILE", "TOP_N_ROW")
    )
  }

  def loadAppConfig(filePath: String): Try[AppConfig] = {
    Try(loadConfigFile(filePath)).flatMap(parseAppConfig)
  }

  def printConfig(config: AppConfig): Unit = {
    println("=" * 50)
    println("Configuration Loaded:")
    println("=" * 50)
    println(s"FILE1:       ${config.file1}")
    println(s"FILE2:       ${config.file2}")
    println(s"OUTPUT_FILE: ${config.outputFile}")
    println(s"TOP_N_ROW:   ${config.topNRow}")

    if (config.additionalProps.nonEmpty) {
      println("\nAdditional Properties:")
      config.additionalProps.foreach { case (k, v) =>
        println(s"  $k = $v")
      }
    }
    println("=" * 50)
  }
}
