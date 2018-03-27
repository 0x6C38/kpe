package parser

import org.apache.spark.sql.{DataFrame, SparkSession}
import parser.Parser.spark

object TanosParser {
  def parseTanos(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.json(path).withColumnRenamed("Kanji", "tanosKanji")
      .withColumnRenamed("jlpt", "tanosJlpt")
      .withColumnRenamed("Kunyomi", "tanosKunyomi")
      .withColumnRenamed("Onyomi", "tanosOnyomi")
      .withColumnRenamed("English", "tanosMeaning")
  }
}