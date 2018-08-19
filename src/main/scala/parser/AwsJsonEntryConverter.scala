package parser

import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

object AwsJsonEntryConverter {
  def convertToAwsJsonEntry(df:DataFrame)(implicit spark:SparkSession):DataFrame = {
    import spark.implicits._
    def jsonToAwsEntry(id:String, jsonFldName:String, json:String):String = {
      "{\"PutRequest\": {\"Item\": {\"id\": {\"S\": \"" + id + "\"},\"" + jsonFldName + "\": {\"S\": \"" + json.replaceAll("\"", "\\\\\"") + "\"}}}},"
    }
    val ujsonToAwsEntry = udf((word:String, json:String) => jsonToAwsEntry(word, jsonFldName = "data", json = json))

    df.withColumn("allZipped", struct(df.columns.head, df.columns.tail: _*))
      .select(ujsonToAwsEntry('word, to_json('allZipped)).as('awsEntry))
  }

}
