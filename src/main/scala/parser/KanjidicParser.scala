package parser

import org.apache.spark.sql.{DataFrame, SparkSession}
import parser.Hello.spark

object KanjidicParser {
  def parseKanjidic(path: String)(implicit spark: SparkSession): DataFrame = {
    //ScalaConfig.kanjidicPath
    spark.read.json(path) //cannot resolve 'UDF(meanings)' due to data type mismatch: argument 1 requires string type, however, '`meanings`' is of array<struct<m_lang:string,meaning:string>> type.;;
      .withColumnRenamed("jlpt", "kdJlpt")
      .withColumnRenamed("meanings", "kdMeanings")
      .withColumnRenamed("readings", "kdReadings")
      .withColumnRenamed("freq", "kdFreq").cache()
  }
}
