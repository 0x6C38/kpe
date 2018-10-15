package parser

import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object AwsJsonEntryConverter {
  def convertToAwsJsonEntry(df:DataFrame, colName:String, prefix:String = "")(implicit spark:SparkSession):DataFrame = {
    import spark.implicits._
    def jsonToAwsEntry(id:String, jsonFldName:String, json:String):String = {
//      "{\"PutRequest\": {\"Item\": {\"id\": {\"S\": \"" + (prefix + id) + "\"},\"" + jsonFldName + "\": {\"S\": \"" + json.replaceAll("\"", "\\\\\"") + "\"}}}},"
      "{\"PutRequest\": {\"Item\": {" + ("\"hk\": {\"S\": \"" + (prefix + id) + "\"},") + ("\"sk\": {\"S\": \"" + (prefix + id) + "\"},") + "\"" + jsonFldName + "\": {\"S\": \"" + json.replaceAll("\"", "\\\\\"") + "\"}}}},"
    }
    val ujsonToAwsEntry = udf((word:String, json:String) => jsonToAwsEntry(word, jsonFldName = "data", json = json))

    df.withColumn("allZipped", struct(df.columns.head, df.columns.tail: _*))
      .select(ujsonToAwsEntry(col(colName), to_json('allZipped)).as('awsEntry))
  }

  def convertWordToAwsJsonEntry(df:DataFrame)(implicit spark:SparkSession):DataFrame = convertToAwsJsonEntry(df, "word", "wrd-")
  def convertKanjiToAwsJsonEntry(df:DataFrame)(implicit spark:SparkSession):DataFrame = convertToAwsJsonEntry(df, "kanji", "knj-")
}
