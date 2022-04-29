package org.example

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{collect_list, struct, to_json}

object Helpers {
  def saveToJson(dfDocs: DataFrame, dfSearches: DataFrame, path: String): Unit =
    dfDocs
      .join(dfSearches, "queryId")
      .select(dfDocs("docId"), dfSearches("date"))
      .groupBy("docId", "date")
      .count
      .groupBy("docId")
      .agg(collect_list(struct("date", "count")).alias("dates"))
      .select(to_json(struct("docId", "dates")).alias("json"))
      .agg(collect_list("json").alias("json_list"))
      .repartition(1)
      .write
      .json(path)
}
