package parser

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{from_json, udf}
import parser.Parser.spark
import spark.implicits._

import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
import sjt.JapaneseSyntax._,  sjt.JapaneseInstances._

import models.{KanaTransliteration, Reading}

case class Example2(example:KanaTransliteration, translation:String)
object KanjiAliveParser {
  def parseKanjiAlive(path: String)(implicit spark: SparkSession): DataFrame = {
    def parseWithCirce(s:String): Seq[Example2] = {
      val decodedFoo = decode[Seq[Seq[String]]](s.replace("\u000D", "").replace("\u000A","")).right.toOption
      decodedFoo.getOrElse(Seq[Seq[String]]()).map(l => Example2(KanaTransliteration(l.head.split("（").head), l.tail.head))
    }

    spark.read.json(Config.KanjiAliveP).withColumnRenamed("kanji", "kaKanji")
      .withColumnRenamed("kmeaning", "kaMeanings")
      .withColumnRenamed("onyomi", "kaOnYomi")
      .withColumnRenamed("kunyomi", "kaKunYomi")
      .withColumnRenamed("onyomi_ja", "kaOnYomi_ja")
      .withColumnRenamed("kunyomi_ja", "kaKunYomi_ja")
//      .withColumn("examplesParsed", removeQuotes('examples))
//    .drop('examples).withColumnRenamed("examplesParsed", "examples")
  }
}
