package parser

import org.apache.spark.sql.{DataFrame, SparkSession}
import parser.Hello.spark

object KanjiAliveParser {
  def parseKanjiAlive(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.json(Config.KanjiAliveP).withColumnRenamed("kanji", "kaKanji")
      .withColumnRenamed("kmeaning", "kaMeanings")
      .withColumnRenamed("onyomi", "kaOnYomi")
      .withColumnRenamed("kunyomi", "kaKunYomi")
      .withColumnRenamed("onyomi_ja", "kaOnYomi_ja")
      .withColumnRenamed("kunyomi_ja", "kaKunYomi_ja")
  }
}
