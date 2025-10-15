case class AppConfig(
                      file1: String,
                      file2: String,
                      outputFile: String,
                      topNRow: Int,
                      additionalProps: Map[String, String] = Map.empty
                    )
