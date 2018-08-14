package parser

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object MeaningCombiner {
  def combineMeanings(kanjidic:DataFrame, kanjiAlive:DataFrame, tanosKanji:DataFrame)(implicit spark:SparkSession):DataFrame = {
    import spark.implicits._

    def combineAllMeanings(meanings: Seq[Row], tanosMeaning:  Seq[Row], kaMeanings:  Seq[Row]): Seq[(String, String)] = {
      val ms = if (meanings != null) meanings.map { case Row(x: String, y: String) => (x, y); case _ => ("", "") } else Seq[(String, String)]()
      val tns = if (tanosMeaning != null) tanosMeaning.map { case Row(x: String, y: String) => (x, y); case _ => ("", "") } else Seq[(String, String)]()
      val kans = if (kaMeanings != null) kaMeanings.map { case Row(x: String, y: String) => (x, y); case _ => ("", "") } else Seq[(String, String)]()
      (ms ++ tns ++ kans).distinct
    }

    val toCombinedMeaningsSet = udf((meanings: Seq[Row], tanosMeaning: Seq[Row], kaMeanings: Seq[Row]) => combineAllMeanings(meanings, tanosMeaning, kaMeanings))

    def parseSimpleEnglish(s: String): Seq[(String, String)] = if (s != null) s.trim.split(", ").map(t => ("en", t)) else Seq[(String, String)]()
    val toTranslationArray = udf((s: String) => parseSimpleEnglish(s))
    //println("kd meanings count: " + kanjidic.filter(r => getFld(r, "kdMeanings") != "").count)
    val combinedMeanings = kanjidic
      .join(kanjiAlive, kanjidic("literal") === kanjiAlive("kaKanji"), "fullouter")
      .join(tanosKanji, kanjidic("literal") === tanosKanji("tanosKanji"), "fullouter")
      .withColumn("meanings", toCombinedMeaningsSet('kdMeanings, toTranslationArray('tanosMeaning), toTranslationArray('kaMeanings)))
      .select('literal, 'meanings)
      .withColumnRenamed("literal", "cmLiteral")
      .alias("combinedMeanings")
    combinedMeanings
  }
}
