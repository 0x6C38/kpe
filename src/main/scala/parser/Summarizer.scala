package parser

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class Transliterations(original: String, katakana:String, hiragana:String, romaji:String)
object Summarizer {
  def summarizeWords(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val uSummarizeArray = udf((xs: Seq[String]) => xs match {
      case null => ""
      case _ => xs.take(Math.min(2, xs.size))
        .mkString(", ")
    })
    val uExtractHiragana= udf((r:Row) => r match {
      case Row(original: String, katakana: String, hiragana:String, romaji:String) => hiragana: String
      case _ => ""
    })
    val uExtractRomaji= udf((r:Row) => r match {
      case Row(original: String, katakana: String, hiragana:String, romaji:String) => romaji: String
      case _ => ""
    })
    df.select('word, uSummarizeArray('translations) as "meaning") //,'totalOcurrences, 'rank, uExtractHiragana('transliterations) as "kana", uExtractRomaji('transliterations) as "romaji"
  }

  def summarizeKanjis(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    ???
  }

}
