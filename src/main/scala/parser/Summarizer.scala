package parser

import models.{KanaTransliteration, Transliteration}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class Transliterations(original: String, katakana: String, hiragana: String, romaji: String)

object Summarizer {
  def summarizeWords(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val uSummarizeArray = udf((xs: Seq[String]) => xs match {
      case null => ""
      case _ => xs.foldLeft("")((z, i) => if ((z ++ i).length <= 30 || z == "") z ++ ", " ++ i else z).mkString.replaceFirst(", ", "") //Bug with some meanings?Joint?  //.take(Math.min(2, xs.size)).mkString(", ")
    })

    val uExtractHiragana = udf((r: Row) => r match {
      case Row(original: String, katakana: String, hiragana: String, romaji: String) => hiragana: String
      case _ => ""
    })

    val uExtractRomaji = udf((r: Row) => r match {
      case Row((original: String, katakana: String, hiragana: String, romaji: String), freq: Int) => romaji: String
      case _ => ""
    })

    df.select('word, uSummarizeArray('translations) as "meaning") //,'totalOcurrences, 'rank, uExtractHiragana('transliterations) as "kana", uExtractRomaji('transliterations) as "romaji"
  }


  val uExtractRomaji = udf((r: Seq[Row]) => r.map {r => r.getAs[(String, String, String, String)](0)._4 }.mkString(""))

  def summarizeKanjis(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val uSummarizeMeaningsArray = udf((xs: Seq[Row]) => xs.filter { case Row(x: String, y: String) => x == "en"; case _ => false }.map { case Row(x: String, y: String) => y; case _ => false }.mkString(", "))
    df.select('kanji, 'rank, uSummarizeMeaningsArray('meanings) as "meanings", uExtractRomaji('kunYomi) as "romajiKun", uExtractRomaji('onYomi) as "romajiOn") //,'totalOcurrences, 'rank, uExtractHiragana('transliterations) as "kana",

  }

}
