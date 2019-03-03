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

    df.select('word as "w", uSummarizeArray('translations) as "m") //,'totalOcurrences, 'rank, uExtractHiragana('transliterations) as "kana", uExtractRomaji('transliterations) as "romaji"
  }


  def summarizeKanjis(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val uExtractRomaji = udf((r: Seq[Row]) => r.map{case Row(kr:Row,n:Long) => kr match {case Row(o:String,h:String, k:String, r:String) => r}}.mkString(", "))
    val uExtractHiragana = udf((r: Seq[Row]) => r.map{case Row(kr:Row,n:Long) => kr match {case Row(o:String,h:String, k:String, r:String) => h}}.mkString(", "))
    val uSummarizeMeaningsArray = udf((xs: Seq[Row]) => xs.filter { case Row(x: String, y: String) => x == "en"; case _ => false }.map { case Row(x: String, y: String) => y; case _ => false }.distinct.mkString(", "))
    df.select('kanji, uSummarizeMeaningsArray('meanings) as "meanings", uExtractRomaji('kunYomi) as "romajiKun", uExtractRomaji('onYomi) as "romajiOn", uExtractHiragana('kunYomi) as "hiraganaKun", uExtractHiragana('onYomi) as "hiraganaOn") //,'totalOcurrences, 'rank,

  }

}
