package parser

import models.KanaTransliteration
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import parser.Hello.spark

object VocabularyParser {
  def parseVocabulary(path: String, edict: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val uBaseForm = udf((word: String) => word.tokenize().headOption.map(_.getBaseForm()).getOrElse(""))
    val uFurigana = udf((word: String) => word.furigana().map(f => (f.original, f.kana.hiragana)))
    val uTransliterate = udf((japanese: String) => KanaTransliteration(japanese): KanaTransliteration)
    val uTransliterateA = udf((js: Seq[String]) => js.map(japanese => KanaTransliteration(japanese): KanaTransliteration))
    val uSum3 = udf((a: Int, b: Int, c: Int) => a + b + c)

    val rawVocabulary = spark.read.json(path) //ScalaConfig.FrequentWordsP
    val vocabulary = rawVocabulary
      .withColumn("internetRank", dense_rank().over(Window.orderBy(col("internetRelative").desc)))
      .withColumn("novelsRank", dense_rank().over(Window.orderBy(col("novelRelative").desc)))
      .withColumn("subtitlesRank", dense_rank().over(Window.orderBy(col("subtitlesRelative").desc)))
      .withColumn("rank", dense_rank().over(Window.orderBy(col("averageRelative").desc)))
      .withColumn("transliterations", (uTransliterate(col("word"))))
      .withColumn("furigana", (uFurigana('word)))
      .withColumn("totalOcurrences", uSum3('internetOcurrences, 'novelOcurrences, 'subtitlesOcurrences))
      .withColumn("baseForm", uBaseForm('word)) //maybe collapse on to base form?
      .join(edict, edict("edictWord") === rawVocabulary("word"), "left").drop('edictWord) //maybe join on baseforms if not found?
      .orderBy('rank)
    vocabulary
  }

}
