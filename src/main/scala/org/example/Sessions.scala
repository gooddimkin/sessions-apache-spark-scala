package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, explode, map_keys}
import org.example.Parser.{getListOfFiles, parseFile}

import java.io.{File, PrintWriter}

object Sessions extends App {
  if (args.length <= 0) {
    println("Укажите путь до сессий")
    System.exit(0)
  };
  val data = getListOfFiles(args(0)).flatMap(parseFile)

  val docOpens =
    data.filter(_.isInstanceOf[DocOpen]).asInstanceOf[List[DocOpen]]

  val searches = data.filter(_.isInstanceOf[Search]).asInstanceOf[List[Search]]

  val path = args(0)
  val spark: SparkSession = SparkSession.builder
    .appName("test")
    .master("local")
    .getOrCreate

  import spark.implicits._
  val dfDocsOpens = docOpens.toDF
    .withColumnRenamed("datetime", "datetime2")
    .withColumnRenamed("date", "date2")

  val dfSearches = searches.toDF

  val searchDocId = "ACC_45616"
  val dfSearchesCard = dfSearches.filter(dfSearches("queryType") === "card")
  val dfDocInResult =
    dfSearchesCard.filter(array_contains(dfSearchesCard("docs"), searchDocId))

  val dfDocInParam = dfDocInResult
    .filter(array_contains(map_keys(dfSearchesCard("params")), "$0"))
    .select(explode(dfSearchesCard("params")))
    .filter(col("key") === "$0" && col("value") === searchDocId)

  val writer = new PrintWriter(new File("output/count.txt"))
  writer.println(
    f"Количество раз, когда в карточке искали документ с идентификатором ${searchDocId}: ${dfDocInParam.count}"
  )
  writer.println(
    f"Количество раз, когда в карточке поиска возвращался документ с идентификатором ${searchDocId}: ${dfDocInResult.count}"
  )
  writer.close()

  val dfSearchesQuick = dfSearches.filter(dfSearches("queryType") === "quick")

  dfDocsOpens
    .join(dfSearchesQuick, "queryId")
    .select(dfDocsOpens("docId"), dfSearchesQuick("date"))
    .groupBy("docId", "date")
    .count
    .orderBy("docId", "date")
    .repartition(1)
    .write
    .option("header", "true")
    .csv("output/csv")

  spark.stop
}
