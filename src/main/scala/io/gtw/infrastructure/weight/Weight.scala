package io.gtw.infrastructure.weight

import com.typesafe.scalalogging.LazyLogging
import io.gtw.infrastructure.weight.utils.JsonUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import scala.collection.JavaConverters._

object Weight extends LazyLogging {
  def main(args: Array[String]) {
    val parser = new OptionParser[Param]("gtw-weight") {
      head("Attribution Module")
      opt[String]('p', "inputWikipedia").action((x, c) => c.copy(inputWikipedia = Some(x))).text("input wikipedia path of the file that need extract attributes from.")
      opt[String]('d', "inputWikidata").action((x, c) => c.copy(inputWikidata = Some(x))).text("input wikidata path.")
      opt[String]('i', "inputBase").action((x, c) => c.copy(inputBase = Some(x))).text("input base. [HDFS address].")
      opt[String]('t', "outputFile").action((x, c) => c.copy(outputFile = Some(x))).text("output weight rdf file path of the result that need put to.")
      opt[String]('o', "outputBase").action((x, c) => c.copy(outputBase = Some(x))).text("output base. [HDFS address].")
      opt[Double]('r', "tolerance").action((x, c) => c.copy(tolerance = x)).text("tolerance of PageRank.")
    }

    parser.parse(args, Param()) match {
      case Some(param) =>
        run(param)
      case None =>
        parser.showUsageAsError()
        logger.error("parser is error.")
    }
  }

  def run(param: Param): Unit = {
    val inputBase: Option[String] = param.inputBase
    val inputWikipedia: Option[String] = param.inputWikipedia
    val inputWikidata: Option[String] = param.inputWikidata
    val outputBase: Option[String] = param.outputBase
    val outputFile: Option[String] = param.outputFile
    val tolerance: Double = param.tolerance

    val inputSource: String = inputBase match {
      case Some(x) => x
      case None => ""
    }

    val inputWikipediaPath: String = inputWikipedia match {
      case Some(x) => x
      case None => throw new IllegalArgumentException(s"set input wikipedia file in command line or set Env")
    }

    val inputWikidataPath: String = inputWikidata match {
      case Some(x) => x
      case None => throw new IllegalArgumentException(s"set input wikidata file in command line or set Env")
    }

    val outputSource: String = outputBase match {
      case Some(x) => x
      case None => ""
    }

    val outputFilePath: String = outputFile match {
      case Some(x) => x
      case None => throw new IllegalArgumentException(s"set output File in command line or set Env")
    }

    val inputWikipediaAddress: String = inputSource + inputWikipediaPath
    val inputWikidataAddress: String = inputSource + inputWikidataPath
    val outputFileAddress: String = outputSource + outputFilePath

    val sparkConf = new SparkConf().setAppName("Weight")
    // val sparkConf = new SparkConf().setAppName("Attribution").setMaster("local[1]")
    val sparkContext = new SparkContext(sparkConf)

    /* -- load wikipedia data and convert every single document from json to format: Map[key, value] -- */
    val wikipediaSourceMapRDD: RDD[Map[String, Any]] = sparkContext.textFile(inputWikipediaAddress).map(jsonToMap)

    /* -- calculating <Weight>: PageRank for every document to generate weight with format: Map[`wid`: `weight`] -- */
    val weightRDD: RDD[(Long, Double)] = PageRank.pageRank(sparkContext, wikipediaSourceMapRDD, tolerance)
    val widWikiTitleRDD: RDD[(Long, String)] = wikipediaSourceMapRDD.map(widWikiTitleMapping)
    val WeightedWidWikiTitleRDD = widWikiTitleRDD.leftOuterJoin(weightRDD)
    val formattedWidWeightKeyedByTitle = WeightedWidWikiTitleRDD.map(formatToByIDWeightKeyedByTitle)

    val wikidataSourceMapRDD = sparkContext.textFile(inputWikidataAddress).map(preprocess).filter(line => line != "").map(jsonToMap).map(cleanWikidata)
    val wikiDataItemRDD = wikidataSourceMapRDD.filter(wikiData => wikiData.getOrElse("type", "") == "item")
    val wikiDataItemSourceKeyedByWikiTitleRDD = wikiDataItemRDD.map(wikidataKeyedByWikiTitle)
    val weightRDFRDD = formattedWidWeightKeyedByTitle.join(wikiDataItemSourceKeyedByWikiTitleRDD).map(x => generateWeightRDF(x._1, x._2)).repartition(5)

    /* -- output to file -- */
    // weightRDFRDD.foreach(println)
    weightRDFRDD.saveAsTextFile(outputFileAddress)
  }

  private def generateWeightRDF(wikiTitle: String, joinedMap: (Map[String, Any], Map[String, Any])): String = {
    val weightMap = joinedMap._1
    val idMap = joinedMap._2
    val id = idMap.getOrElse("id", "00")
    val weight = "\"" + weightMap.getOrElse("weight", "0.15").toString + "\""
    s"<http://www.wikidata.org/entity/$id> <graph-the-world-weight> $weight ."
  }

  private def preprocess(line: String): String = {
    if (line != "[" && line != "]") {
      if (line.charAt(line.length() - 1) == ',') {
        line.substring(0, line.length - 1)
      } else {
        line
      }
    } else {
      ""
    }
  }

  private def jsonToMap(line: String): Map[String, Any] = {
    val sourceMap: Map[String, Any] = JsonUtils.parseJson(line)
    sourceMap
  }

  private def widWikiTitleMapping(sourceMap: Map[String, Any]): (Long, String) = {
    val wikiTitle = sourceMap.getOrElse("title", "xxx").toString
    val wid = sourceMap.getOrElse("wid", "0").toString.toLong
    (wid, wikiTitle)
  }

  private def formatToByIDWeightKeyedByTitle(result: (Long, (String, Option[Double]))): (String, Map[String, Any]) = {
    val formattedResultMap = new scala.collection.mutable.HashMap[String, Any]()
    result match {
      case (wid, (wikiTitle, weight)) =>
        formattedResultMap += ("wid" -> wid)
        formattedResultMap += ("title" -> wikiTitle)
        formattedResultMap += ("weight" -> weight.getOrElse(0.15))
        (wikiTitle, formattedResultMap.toMap)
    }
  }

  def wikidataKeyedByWikiTitle(wikidataMap: Map[String, Any]): (String, Map[String, Any]) = {
    val siteLinks = wikidataMap.get("sitelinks")
    siteLinks match {
      case Some(siteLinksInfo) =>
        val siteLinksInfoMap: Map[String, Any] = siteLinksInfo match {
          case x: Map[String, Any] => x
          case x: java.util.LinkedHashMap[String, Any] => x.asScala.toMap
          case _ => Map[String, Any]()
        }
        siteLinksInfoMap.get("enwiki") match {
          case Some(enwikiInfo) =>
            val wikiTitle = enwikiInfo match {
              case enwikiInfoX: Map[String, String] =>
                enwikiInfoX.get("title") match {
                  case Some(title) => title
                }
              case enwikiInfoX: java.util.LinkedHashMap[String, String] =>
                enwikiInfoX.asScala.get("title") match {
                  case Some(title) => title
                }
              case _ => ""
            }
            (wikiTitle, wikidataMap)
          case None => ("", Map[String, Any]())
        }
      case None => ("", Map[String, Any]())
    }
  }

  def cleanWikidata(wikidataSourceMap: Map[String, Any]): Map[String, Any] = {
    wikidataSourceMap.-("labels", "descriptions", "aliases")
  }
}
