package io.gtw.infrastructure.weight

import io.gtw.infrastructure.weight.utils.EnvUtils

case class Param (
                   inputWikipedia: Option[String] = EnvUtils.inputWikipediaFromEnv,
                   inputWikidata: Option[String] = EnvUtils.inputWikidataFromEnv,
                   inputBase: Option[String] = EnvUtils.inputBaseFromEnv,
                   outputFile: Option[String] = EnvUtils.outputFileFromEnv,
                   outputBase: Option[String] = EnvUtils.outputBaseFromEnv,
                   tolerance: Double = EnvUtils.toleranceFromEnv
                 )

