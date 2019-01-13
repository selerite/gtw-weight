package io.gtw.infrastructure.weight.utils

import scala.util.Properties

object EnvUtils {
  def inputBaseFromEnv: Option[String] = Properties.envOrNone("WEIGHT_INPUT_BASE")
  def inputWikipediaFromEnv: Option[String] = Properties.envOrSome("WEIGHT_INPUT_WIKIPEDIA", Some("D:\\self\\gtw\\data\\wikipedia_head_less_json_lines.json"))
  def inputWikidataFromEnv: Option[String] = Properties.envOrSome("WEIGHT_INPUT_WIKIDATA", Some("D:\\self\\gtw\\data\\wikidata_head200_with_1_modified.json"))
  def outputBaseFromEnv: Option[String] = Properties.envOrNone("WEIGHT_OUTPUT_BASE")
  def outputFileFromEnv: Option[String] = Properties.envOrSome("WEIGHT_OUTPUT_FILE", Some("xxx"))
  def toleranceFromEnv: Double = Properties.envOrElse("WEIGHT_TOLERANCE", "0.01").toDouble
}
