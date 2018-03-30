package parser

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.util.{Failure, Success, Try}
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{length, trim, when}

import models._
import sjt.JapaneseSyntax._
import sjt.JapaneseInstances._

//TODO: Export to Elasticsearch
//TODO: Consolidate readings extracted from vocabulary (w/frequency) with readings extracted from dictionaries

//TODO: Add resource files to build
//TODO: Add more info to the vocabs including: rankOfKanjis(?)
//TODO: Get recursive components for kanjis with their ranks and the ranks of their readings
//TODO: Fix radical column
//TODO: Write final vocab to file
//TODO: Export kanjis

object Parser {
  //val logFile = "/opt/spark-2.1.0-bin-hadoop2.7/README.md" // Should be some file on your system
  implicit val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
  val conf = new SparkConf().setMaster("local[*]").setAppName("Simple Application")

  import spark.implicits._ //necesary import

  //To reduce spark output
  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def extractVocabsForKanji(vocabulary:DataFrame): DataFrame = {
    def getFld(r: Row, name: String) = r.getString(r.fieldIndex(name))
    def filterWord(r: Row): String = getFld(r, "word")

    def containsKanjiFilter(r: Row): Boolean = filterWord(r).containsKanji
    val uExtractKanjiFromVocab = udf((word:String) => word.extractUniqueKanji.map(_.toString).toSeq)

    val vocabPerKanji: Dataset[Row] = vocabulary.filter(r => containsKanjiFilter(r)) //.filter(r => containsJoyoKFilter(r)) //not worth
      .withColumn("vocabKanji", uExtractKanjiFromVocab('word))
      .withColumn("vocabZipped", struct(vocabulary.columns.head, vocabulary.columns.tail: _*))
      .select('word, explode('vocabKanji) as "vocabK", 'vocabZipped)
      .groupBy('vocabK)
      .agg(collect_list('vocabZipped) as "vocabsPerKanji")
    vocabPerKanji
  }
  def extractKanjiPerVocab(vocabulary:DataFrame, kanjis:DataFrame):DataFrame = {
    def getFld(r: Row, name: String) = r.getString(r.fieldIndex(name))
    def filterWord(r: Row): String = getFld(r, "word")
    def containsKanjiFilter(r: Row): Boolean = filterWord(r).containsKanji
    val uExtractKanjiFromVocab = udf((word:String) => word.extractUniqueKanji.map(_.toString).toSeq)

    val kanjiPerVocab: Dataset[Row] = vocabulary.filter(r => containsKanjiFilter(r))
      .withColumn("vocabKanji", uExtractKanjiFromVocab('word))
      .select('*, explode('vocabKanji) as "vocabK")
      .join(kanjis.withColumn("kanjiZipped", struct(kanjis.columns.head, kanjis.columns.tail: _*)).select('kanji, 'kanjiZipped), 'vocabK === kanjis("kanji"), "left")
      .drop('vocabKanji)
      .drop('vocabK)
      .groupBy('word)
      .agg(collect_list('kanjiZipped) as "kanjisInVocab")
      .withColumnRenamed("word", "wordK")
    kanjiPerVocab
  }
  def printInfo(df: DataFrame, name: String = "")(numberToShow: Int = 50, count: Boolean = true, schema: Boolean = false) = {
    if (count) println(s"$name DF has a total of ${df.count()} rows.")
    if (numberToShow > 0){
      println(s"$name DF, first $numberToShow rows:")
      df.show(numberToShow)
    }
    if (schema) {
      println(s"$name DF Schema:")
      df.printSchema()
    }
  }

  def main(args: Array[String]): Unit = {
    def read(path: String): Try[DataFrame] = Try(spark.read.parquet(path))

    def parseAll = {
      //--- Errors ---
      //val radicals = spark.read.json(ScalaConfig.KanjiAliveRadicalP) //radical isn't properly encoded in file it seems //EN EL ARCHIVO ORIGINAL POR ESO
      //val kanjiVG = spark.read.json(ScalaConfig.KanjiVGDP) //returns empty

      val wikiRadicals = spark.read.json(Config.WikiRadsDP)

      val lvlsRaw = spark.read.json(Config.levelsPath).cache()
      printInfo(lvlsRaw, "LvlsRaw")()

      //    val translationsDictionary = spark.read.json(ScalaConfig.JmDicP) //incorrect formatting //Should eventually use instead of EDICT
      val edict = LocalCache.of(Config.Edict, EdictParser.parseEdict(Config.Edict), true)
      printInfo(edict, "Edict")()

      val kanjiFreqs = FreqParser.parseAll(Config.aoFreq, Config.twitterFreq, Config.wikipediaFreq, Config.newsFreq, Config.allFreqs)
      printInfo(kanjiFreqs, "Kanji Freqs")()

      val kanjidic = LocalCache.of(Config.kanjidicPath, KanjidicParser.parseKanjidic(Config.kanjidicPath), true)
      printInfo(kanjidic, "Kanjidic")()

      val kanjiAlive = LocalCache.of(Config.KanjiAliveP,KanjiAliveParser.parseKanjiAlive(Config.KanjiAliveP), true)
      printInfo(kanjiAlive, "KanjiAlive")()

      val tanosKanji = LocalCache.of(Config.KanjiTanosPFreq, TanosParser.parseTanos(Config.KanjiTanosPFreq), true)
      printInfo(tanosKanji, "Tanos Kanji")()

      val tatoes = spark.read.json(Config.TatoebaDP)
      printInfo(tatoes, "Tatoes Kanji")()

      val rawComps = spark.read.textFile(Config.CompositionsPath).filter(l => l.startsWith(l.head + ":") && l.head.isKanji)
      val comps = rawComps.map(l => l.head.toString -> Composition.parseKCompLine(l))
        .withColumnRenamed("_1", "cKanji")
        .withColumnRenamed("_2", "components")

      val allFragmentsLists = spark.read.option("delimiter", ":").format("csv").load(Config.KradFN)
        .withColumnRenamed("_c0", "fKanji").withColumnRenamed("_c1", "ffragments")
        .withColumn("fKanji", trim(col("fKanji"))).withColumn("ffragments", trim(col("ffragments"))) //must trim to match

      val combinedMeanings = LocalCache.of(Config.mCombinedP, MeaningCombiner.combineMeanings(kanjidic, kanjiAlive, tanosKanji), true)
      printInfo(combinedMeanings, "Meanings")()

      val vocabulary = LocalCache.of(Config.vocabPath, VocabularyParser.parseVocabulary(Config.FrequentWordsP, edict), true).cache()
      printInfo(vocabulary, "Vocabulary")(500)

      val kanjiReadings = ReadingParser.inferReadingsFromVocab(vocabulary) //rename inferedReadings
      printInfo(kanjiReadings, "KanjiReadings")()

      val dicReadings = ReadingParser.parseReadingsFromDictionaries(lvlsRaw,kanjidic, kanjiAlive, tanosKanji)
      printInfo(dicReadings, "dicReadings")()

      val readingsDF = ReadingParser.combineInferedReadingsWithDicReadings(kanjiReadings, dicReadings)
      printInfo(readingsDF, "Readings")()

      // --- Final Data Joins ---
    val rawJointDF = lvlsRaw.alias("levelRaw").join(kanjidic, lvlsRaw("kanji") === kanjidic("literal"), "left")
      .join(allFragmentsLists, lvlsRaw("kanji") === allFragmentsLists("fKanji"), "left")
      .join(tanosKanji, lvlsRaw("kanji") === tanosKanji("tanosKanji"), "left")
      .join(kanjiAlive, lvlsRaw("kanji") === kanjiAlive("kaKanji"), "left")
      .join(comps, lvlsRaw("kanji") === comps("cKanji"), "left")
      .join(kanjiFreqs, lvlsRaw("kanji") === kanjiFreqs("freqKanji"), "left")
      .join(combinedMeanings, col("levelRaw.kanji") === col("combinedMeanings.cmLiteral"), "left")
      .join(readingsDF, col("levelRaw.kanji") === readingsDF("readingsKanji"), "left")
      .join(kanjiReadings, lvlsRaw("kanji") === kanjiReadings("k"))
      .cache
    //.join(vocabSpark, lvlsRaw("kanji") === vocabSpark("_1"),"left") //Correct _1 name //*
    rawJointDF.show(22)

    val jointDF = rawJointDF.drop(col("fragments"))
      .drop(col("isEUCJP")).drop(col("isKANGXI")).drop(col("isKanji"))
      .drop(col("literal"))
      .drop(col("processedRadicals"))
      .drop(col("fKanji")).drop(col("ffragments"))
      .drop(col("tanosKanji")).drop(col("kaKanji")).drop(col("cKanji")).drop(col("freqKanji")).drop(col("twKanji")).drop(col("wkKanji")).drop(col("newsKanji"))
      .drop(col("tanosJlpt")).drop(col("kdJlpt"))
      .drop(col("tanosKunyomi")).drop(col("tanosOnyomi"))
      .drop(col("kgrade")).drop(col("kstroke"))
      .drop('kdMeanings) //drops redundant meanings columns
      .drop('tanosMeaning).drop('kaMeanings)
      .drop("cmLiteral")
      .drop('readingsKanji) //drops redundant readings columns
      .drop(col("kdReadings")).drop('readings)
      .drop(col("kaKunYomi_ja")).drop(col("kaOnYomi_ja")).drop(col("kaKunYomi")).drop(col("kaOnYomi"))
      .drop('k)
      .drop('kdFreq)
    //.orderBy(col("jlpt")) //can't resolve
    jointDF.show(23)

    val kanjis = jointDF.drop(col("dic_numbers"))
      .drop(col("query_codes"))
      .orderBy(col("rank"))

    kanjis.show(50)

    println("TrimmedDF Count: " + kanjis.count()) //expensive

    // IMPORTANT !! UDF MUST NOT THROW ANY INTERNAL EXCEPTIONS; THAT INCLUDES NULL OR THEY WONT WORK
    readingsDF.show(20)
    println("Number of readings: " + readingsDF.count()) //expensive

    //PROBLEM after flatmaping jcommas
    val uMkStr = udf((a: Seq[String]) => a.mkString(","))

    //Writes to File
    //Writes Readings
    readingsDF.select('readingsKanji, uMkStr('readings) as "readings").coalesce(1).write.mode(SaveMode.Overwrite).csv("outputSF") //readings.csv

    (vocabulary, kanjis)
  }

    //START REFACTORING CODE
    val edict = LocalCache.of(Config.Edict, EdictParser.parseEdict(Config.Edict), true)
    val vocabulary2 = LocalCache.of(Config.vocabPath, VocabularyParser.parseVocabulary(Config.FrequentWordsP, edict), true).cache()
//    printInfo(vocabulary2, "Vocabulary")(50, schema = true)

    //INFER FROM VOCAB
    val uExtractKanji = udf((r: Row) => r match {
      case Row(x: String, y: String) => x: String
      case _ => ""
    })

    val kanjiReadings = vocabulary2.select('word, 'totalOcurrences, explode('furigana) as "furigana")
      .groupBy('furigana)
      .agg(sum('totalOcurrences) as "Occ")
      .orderBy('furigana)
      .withColumn("k", uExtractKanji('furigana))
      .groupBy('k)
      .agg(collect_list(struct('furigana, 'Occ)) as "readingsWFreq") //if doesn't work remove struct
    printInfo(kanjiReadings, "KanjiReadings")(50, true, true)

    //REPLACEMENT
    val kanjiReadings2 = ReadingParser.inferReadingsFromVocab(vocabulary2) //rename inferedReadings
    printInfo(kanjiReadings2, "KanjiReadings2")(50, true, true)
    //REPLACEMENT
    /// END INFER FROM VOCAB

    //Readings from dics
    val lvlsRaw = spark.read.json(Config.levelsPath).cache()
//    printInfo(lvlsRaw, "LvlsRaw")()

    val kanjidic = LocalCache.of(Config.kanjidicPath, KanjidicParser.parseKanjidic(Config.kanjidicPath), true)
//    printInfo(kanjidic, "Kanjidic")()

    val kanjiAlive = LocalCache.of(Config.KanjiAliveP,KanjiAliveParser.parseKanjiAlive(Config.KanjiAliveP), true)
//    printInfo(kanjiAlive, "KanjiAlive")()

    val tanosKanji = LocalCache.of(Config.KanjiTanosPFreq, TanosParser.parseTanos(Config.KanjiTanosPFreq), true)
//    printInfo(tanosKanji, "Tanos Kanji")()

    //FROM HERE DICS PARSE
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

    def composeWFreqs(kWFreq: Seq[(String, String, Long)], raws: Seq[KanaTransliteration]) = {
      raws.map(k => (k, kWFreq.groupBy(_._2).get(k.hiragana))).filter(_._2.isDefined).map(t => Reading(t._1, t._2.get.head._3)).distinct
    }

    val udfComposeReadings = udf((kWFreq: Seq[Row], raws: Seq[Row]) => {
      val ts = kWFreq.map { case Row(kr: Row, n: Long) => kr match { case Row(k: String, r: String) => (k, r, n) }
      }
      val rs = raws.map { case Row(original:String, hiragana:String, katakana:String, romaji:String) => KanaTransliteration.apply(original, hiragana, katakana, romaji)}
      composeWFreqs(ts, rs)
    })

    val dicReadings = lvlsRaw.join(kanjidic, lvlsRaw("kanji") === kanjidic("literal"), "left")
      .join(kanjiAlive, lvlsRaw("kanji") === kanjiAlive("kaKanji"), "left")
      .join(tanosKanji, lvlsRaw("kanji") === tanosKanji("tanosKanji"), "left") //not taken into consideration yet
      .select('kanji as "readingsKanji", mapKDReadings('kdReadings, 'kaKunYomi_ja, 'kaOnYomi_ja, 'tanosKunyomi, 'tanosOnyomi) as "readings", uTransliterateA(uMapKDReadingsKun('kdReadings, 'kaKunYomi_ja, 'tanosKunyomi)) as "kunYomiRaw", uTransliterateA(uMapKDReadingsOn('kdReadings, 'kaOnYomi_ja, 'tanosOnyomi)) as "onYomiRaw")
    printInfo(dicReadings, "dicReadings")(100, true, true)

    //REPLACEMENT
    val dicReadings2 = ReadingParser.parseReadingsFromDictionaries(lvlsRaw,kanjidic, kanjiAlive, tanosKanji)
    printInfo(dicReadings2, "dicReadings2")(100, true, true)
    //END REPLACEMENT

      val readingsDF = dicReadings
      .join(kanjiReadings, col("readingsKanji") === kanjiReadings("k"), "left") //TO BEFORE HERE
      .withColumn("kunYomi", udfComposeReadings('readingsWFreq, 'kunYomiRaw))
      .withColumn("onYomi", udfComposeReadings('readingsWFreq, 'onYomiRaw))
      .drop('kunYomiRaw).drop('onYomiRaw).drop('k)

    readingsDF.show(99, false)
    printInfo(readingsDF, "ReadingsJoint")(100, true, true)
    val readingsDF2 = ReadingParser.combineInferedReadingsWithDicReadings(kanjiReadings2, dicReadings2)
    readingsDF2.show(99, false)
    printInfo(readingsDF2, "ReadingsJoint 2")(100, true, true)

    val readingsDF3 = ReadingParser.parseAllReadings(vocabulary2, lvlsRaw, kanjidic, kanjiAlive, tanosKanji)
    readingsDF3.show(99, false)
    printInfo(readingsDF3, "ReadingsJoint 3")(100, true, true)


    //END REFACTORING CODE

    //Parse the thing
    println(Config.vocabCacheFN)
    println(Config.kanjiCacheFN)
    val (vocabulary:DataFrame, kanjis:DataFrame) = (read(Config.vocabCacheFN), read(Config.kanjiCacheFN)) match {
      case (Success(vocab), Success(kanjis)) => (vocab, kanjis)
      case _ => parseAll
    }

    printInfo(kanjis, "Kanjis")(50, true, true)
    printInfo(vocabulary, "Vocabulary")(50, true, true)

    println("-- joining vocabs <-> kanji-- ")

    val vocabPerKanji = extractVocabsForKanji(vocabulary)

    val jointKV = kanjis.join(vocabPerKanji, kanjis("kanji") === vocabPerKanji("vocabK"), "left").drop('vocabK)
    jointKV.show(50)


    val kanjiPerVocab = extractKanjiPerVocab(vocabulary, kanjis)
    kanjiPerVocab.show(49, false)

    val jointVK = vocabulary.join(kanjiPerVocab, kanjiPerVocab("wordK") === vocabulary("word")).drop('wordK)
    jointVK.show(48, false)

    /* Commented for dealing with cache
    //Writes Kanji (multiple files)
    kanjis.write.mode(SaveMode.Overwrite).json("output")
    //Writes Kanji (single file)
    kanjis.coalesce(1).write.mode(SaveMode.Overwrite).json("outputSF")
    kanjis.coalesce(1).write.mode(SaveMode.Overwrite).parquet(ScalaConfig.kanjiCache)
    //Writes vocabulary (potencially huge, must check)
    //vocabulary.coalesce(1).write.mode(SaveMode.Overwrite).json("vocab")
    vocabulary.coalesce(1).write.mode(SaveMode.Overwrite).parquet(ScalaConfig.vocabCache)
*/
    spark.stop
  }
}
