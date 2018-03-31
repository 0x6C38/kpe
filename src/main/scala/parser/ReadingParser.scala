package parser

import models.{KanaTransliteration, Reading}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import sjt.JapaneseSyntax._
import sjt.JapaneseInstances._
import parser.Parser.spark

object ReadingParser {
  import spark.implicits._
  def inferReadingsFromVocab(vocabulary:DataFrame)(implicit spark: SparkSession): DataFrame = {
    val uExtractKanji = udf((r: Row) => r match {
      case Row(x: String, y: String) => x: String
      case _ => ""
    })

    vocabulary.select('word, 'totalOcurrences, explode('furigana) as "furigana")
      .groupBy('furigana)
      .agg(sum('totalOcurrences) as "Occ")
      .orderBy('furigana)
      .withColumn("k", uExtractKanji('furigana))
      .groupBy('k)
      .agg(collect_list(struct('furigana, 'Occ)) as "readingsWFreq")
  }
  def parseReadingsFromDictionaries(lvlsRaw:DataFrame, kanjidic:DataFrame, kanjiAlive:DataFrame, tanosKanji:DataFrame)(implicit spark: SparkSession): DataFrame = {
    def mapKDReadingsKun(rs: Seq[Row], k: String, t: String) = {
      val kuns = if (k != null && k.trim != "") k.split("、") else Array[String]() //.map(_.toHiragana())
      val tkuns = if (k != null && k.trim != "") k.split(" ").flatMap(_.split("、")) else Array[String]()
      val kdicsKun = rs.flatMap {
        case Row(x: String, y: String) if (x == "ja_kun" && y != "") => (Some(y)) // .replace("-", "") //ignores endings & positions in kanjidic
        case _ => None
      }
      kuns ++ kdicsKun.map(_.split('.').head) ++ tkuns.map(_.split('.').head)
    }

    val uMapKDReadingsKun = udf((rs: Seq[Row], k: String, t: String) => mapKDReadingsKun(rs, k, t))

    def mapKDReadingsOn(rs: Seq[Row], y: String, t: String) = {
      val ons = if (y != null && y.trim != "") y.split("、") else Array[String]()
      val tons = if (y != null && y.trim != "") y.split(" ").flatMap(_.split("、")) else Array[String]()
      val kdicsOn = rs.flatMap {
        case Row(x: String, y: String) if (x == "ja_on" && y != "") => (Some(y)) // .replace("-", "") //ignores endings & positions in kanjidic
        case _ => None
      }
      ons ++ kdicsOn.map(_.split('.').head) ++ tons.map(_.split('.').head)
    }

    val uMapKDReadingsOn = udf((rs: Seq[Row], k: String, t: String) => mapKDReadingsOn(rs, k, t))

    def cleanUpR(rs: Seq[String]): Seq[String] = rs.map(_.replace("-", "")).distinct

    val mapKDReadings = udf((rs: Seq[Row], k: String, y: String, tk: String, to: String) => {
      val kunYomi = mapKDReadingsKun(rs, k, tk)
      val onYomi = mapKDReadingsOn(rs, y, to)
      cleanUpR((kunYomi ++ onYomi).map(_.toHiragana())) //.toSet.toSeq
    }
    )
    // End Readings from dics

    // -- Reading joins --
    val uTransliterateA = udf((js: Seq[String]) => js.map(japanese => KanaTransliteration(japanese): KanaTransliteration))

    lvlsRaw.join(kanjidic, lvlsRaw("kanji") === kanjidic("literal"), "left")
      .join(kanjiAlive, lvlsRaw("kanji") === kanjiAlive("kaKanji"), "left")
      .join(tanosKanji, lvlsRaw("kanji") === tanosKanji("tanosKanji"), "left") //not taken into consideration yet
      .select('kanji as "readingsKanji", mapKDReadings('kdReadings, 'kaKunYomi_ja, 'kaOnYomi_ja, 'tanosKunyomi, 'tanosOnyomi) as "readings", uTransliterateA(uMapKDReadingsKun('kdReadings, 'kaKunYomi_ja, 'tanosKunyomi)) as "kunYomiRaw", uTransliterateA(uMapKDReadingsOn('kdReadings, 'kaOnYomi_ja, 'tanosOnyomi)) as "onYomiRaw")
  }

  def combineInferedReadingsWithDicReadings(inferedReadings: DataFrame, dictionaryReadings: DataFrame)(implicit spark: SparkSession): DataFrame = {
    def composeWFreqs(kWFreq: Seq[(String, String, Long)], raws: Seq[KanaTransliteration]) = {
      raws.map(k => (k, kWFreq.groupBy(_._2).get(k.hiragana))).filter(_._2.isDefined).map(t => Reading(t._1, t._2.get.head._3)).distinct
    }

    val udfComposeReadings = udf((kWFreq: Seq[Row], raws: Seq[Row]) => {
      val ts = kWFreq.map { case Row(kr: Row, n: Long) => kr match {
        case Row(k: String, r: String) => (k, r, n)
      }
      }
      val rs = raws.map { case Row(original: String, hiragana: String, katakana: String, romaji: String) => KanaTransliteration.apply(original, hiragana, katakana, romaji) }
      composeWFreqs(ts, rs)
    })
    dictionaryReadings.join(inferedReadings, col("readingsKanji") === inferedReadings("k"), "left")
      .withColumn("kunYomi", udfComposeReadings('readingsWFreq, 'kunYomiRaw))
      .withColumn("onYomi", udfComposeReadings('readingsWFreq, 'onYomiRaw))
      .drop('kunYomiRaw).drop('onYomiRaw).drop('k).drop('readingsWFreq)
  }

  def parseAllReadings(vocabulary: DataFrame, lvlsRaw: DataFrame, kanjidic: DataFrame, kanjiAlive: DataFrame, tanosKanji: DataFrame)(implicit spark: SparkSession): DataFrame = {
    combineInferedReadingsWithDicReadings(inferReadingsFromVocab(vocabulary), parseReadingsFromDictionaries(lvlsRaw, kanjidic, kanjiAlive, tanosKanji))
  }

}
