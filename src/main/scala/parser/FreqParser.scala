package parser

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import com.atilika.kuromoji.ipadic.Tokenizer
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import parser.Hello.spark

import org.apache.spark.sql.functions._

object FreqParser {
  def parseAOFreqs(path: String): DataFrame = {
    spark.read.option("inferSchema", "true").csv(path)
      .withColumnRenamed("_c0", "freqKanji")
      .withColumnRenamed("_c1", "aoOcu")
      .withColumnRenamed("_c2", "aoFreq")
      .withColumn("aoRank", dense_rank().over(Window.orderBy(col("aoOcu").desc)))
  }

  def parseTwitterFreqs(path: String): DataFrame = {
    spark.read.option("inferSchema", "true").csv(path)
      .withColumnRenamed("_c0", "twKanji")
      .withColumnRenamed("_c1", "twOcu")
      .withColumnRenamed("_c2", "twFreq")
      .withColumn("twRank", dense_rank().over(Window.orderBy(col("twOcu").desc)))
  }

  def parseWikipediaFreqs(path: String): DataFrame = {
    spark.read.option("inferSchema", "true").csv(ScalaConfig.wikipediaFreq)
      .withColumnRenamed("_c0", "wkKanji")
      .withColumnRenamed("_c1", "wkOcu")
      .withColumnRenamed("_c2", "wkFreq")
      .withColumn("wkRank", dense_rank().over(Window.orderBy(col("wkOcu").desc)))
  }

  def parseNewsFreqs(path: String): DataFrame = {
    spark.read.option("inferSchema", "true").csv(path)
      .withColumnRenamed("_c0", "newsKanji")
      .withColumnRenamed("_c1", "newsOcu")
      .withColumnRenamed("_c2", "newsFreq")
      .withColumn("newsRank", dense_rank().over(Window.orderBy(col("newsOcu").desc)))
  }

  def parseAll(aoPath: String, twitterPath: String, wikiPath: String, newsPath: String, path: String): DataFrame = {
    //    ScalaConfig.aoFreq, ScalaConfig.twitterFreq, ScalaConfig.wikipediaFreq, ScalaConfig.newsFreq, ScalaConfig.allFreqs
    val aofreqs = parseAOFreqs(aoPath)

    val twitterFreqs = parseTwitterFreqs(twitterPath)

    val wikipediaFreqs = parseWikipediaFreqs(wikiPath)

    val newsFreqs = parseNewsFreqs(newsPath)

    val avgRankings = udf((first: String, second: String, third: String, fourth: String) => (first.toInt + second.toInt + third.toInt + fourth.toInt).toDouble / 4)

    val kanjiFreqs = aofreqs.join(twitterFreqs, aofreqs("freqKanji") === twitterFreqs("twKanji"))
      .join(wikipediaFreqs, aofreqs("freqKanji") === wikipediaFreqs("wkKanji"))
      .join(newsFreqs, aofreqs("freqKanji") === newsFreqs("newsKanji"))
      .withColumn("avgRank", avgRankings(col("newsRank"), col("wkRank"), col("twRank"), col("aoRank")))
      .withColumn("rank", dense_rank().over(Window.orderBy(col("avgRank").asc)))
    kanjiFreqs
  }
}
