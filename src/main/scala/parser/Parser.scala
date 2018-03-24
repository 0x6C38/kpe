package parser

import java.io.{FileReader, FileWriter, InputStreamReader}
import java.lang.reflect.Type

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import com.atilika.kuromoji.ipadic.Tokenizer
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import com.databricks.spark.xml._
import org.apache.commons.lang3.StringUtils

import scala.util.{Failure, Success, Try}


//import bayio.kpe._
//import bayio.utils.Config
//import kanjivg.KanjiVGPartsParser
import models.{FrequentWordRawParse, KanjiLevel}
import models._
import org.apache.spark._
import org.apache.spark.sql.functions._
import sjt._
import sjt.JapaneseInstances._
import sjt.JapaneseSyntax._
import org.apache.spark.sql.functions.{length, trim, when}
import org.apache.spark.sql.Column
import org.apache.log4j.{Level, Logger}


//TODO: Export to Elasticsearch

//TODO: Fix Kun/onYomi shit

//TODO: Add resource files to build
//TODO: Add more info to the vocabs including: rankOfKanjis(?)
//TODO: Get recursive components for kanjis
//TODO: Add ranks of components for kanjis
//TODO: Add ranks of readings for kanjis
//TODO: Fix radical column
//TODO: Write final vocab to file
//TODO: Export kanjis

object Hello {
  val conf = new SparkConf().setMaster("local[*]").setAppName("Simple Application")
  val spark: SparkSession = SparkSession.builder.master("local").getOrCreate

  import spark.implicits._ //necesary import

  //To reduce spark output

  import org.apache.log4j.{Level, Logger}

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)


  def extractKanjiFromVocabulary(vs: Dataset[Row]): Array[(FrequentWordRawParse, Array[Char])] = {
    val vParsed: Array[FrequentWordRawParse] = vs.as[FrequentWordRawParse].collect()
    val kanjiPerVocab: Array[(FrequentWordRawParse, Array[Char])] = vParsed.map((w: FrequentWordRawParse) => (w, w.word.extractKanji.toCharArray))
    kanjiPerVocab
  }

  val extractKanjiFromVocab = udf((word: String) => word.extractKanji.map(_.toString))

  //val avgRankings = udf( (first: String, second: String, third:String, fourth:String) =>  (first.toInt + second.toInt + third.toInt  + fourth.toInt).toDouble / 4 )
  /*def extractVocabularyForKanji(vs:Array[(FrequentWordRawParse, List[Char])]):Array[(Char, Array[FrequentWordRawParse])] = {}*/

  def main(args: Array[String]): Unit = {
    //val logFile = "/opt/spark-2.1.0-bin-hadoop2.7/README.md" // Should be some file on your system
    //val conf = new SparkConf().setMaster("local[*]").setAppName("Simple Application")
    def read(path: String): Try[DataFrame] = Try(spark.read.parquet(path))

    def filterWord(r: Row): String = getFld(r, "word")
    def getFld(r: Row, name: String) = r.getString(r.fieldIndex(name))
    def parseAll = {
    //--- Kanji Frequency ---
//    val toDbl = udf[Double, String](_.toDouble)

      // EDICT PARSER START
      //    val edict = EdictParser.parseEdict(ScalaConfig.Edict)
      val edict = LocalCache.of(ScalaConfig.Edict, EdictParser.parseEdict(ScalaConfig.Edict), true)
      edict.show(50)
      // EDICT PARSER END

    edict.show(200, false)
    println(edict.count())

//    val translationsDictionary = spark.read.json(ScalaConfig.JmDicP) //incorrect formatting //Should eventually use instead of EDICT

      // FREQS PARSER START
      val kanjiFreqs = FreqParser.parseAll(ScalaConfig.aoFreq, ScalaConfig.twitterFreq, ScalaConfig.wikipediaFreq, ScalaConfig.newsFreq, ScalaConfig.allFreqs)
      kanjiFreqs.show(52)
      println(kanjiFreqs.count)
      // FREQS PARSER END

    //--- Kanji Composition ---
    val rawComps = spark.read.textFile(ScalaConfig.CompositionsPath).filter(l => l.startsWith(l.head + ":") && l.head.isKanji)
    val comps = rawComps.map(l => l.head.toString -> Composition.parseKCompLine(l))
      .withColumnRenamed("_1", "cKanji")
      .withColumnRenamed("_2", "components")
    //val fivecomps = comps.take(50)

    val lvlsRaw = spark.read.json(ScalaConfig.levelsPath)
//    val lvls = lvlsRaw.as[KanjiLevel].collect()

    val kanjidic = spark.read.json(ScalaConfig.kanjidicPath) //cannot resolve 'UDF(meanings)' due to data type mismatch: argument 1 requires string type, however, '`meanings`' is of array<struct<m_lang:string,meaning:string>> type.;;
      .withColumnRenamed("jlpt", "kdJlpt")
      .withColumnRenamed("meanings", "kdMeanings")
      .withColumnRenamed("readings", "kdReadings")
      .withColumnRenamed("freq", "kdFreq").cache()

    kanjidic.show(7) //reading:ア, r_type:ja_on r_type:ja_kun
    println("Number of kanjidic: " + kanjidic.count()) //expensive

    val allFragmentsLists = spark.read.option("delimiter", ":").format("csv").load(ScalaConfig.KradFN)
      .withColumnRenamed("_c0", "fKanji").withColumnRenamed("_c1", "ffragments")
      .withColumn("fKanji", trim(col("fKanji"))).withColumn("ffragments", trim(col("ffragments"))) //must trim to match


    def parseSimpleEnglish(s: String): Seq[(String, String)] = if (s != null) s.trim.split(", ").map(t => ("en", t)) else Seq[(String, String)]()

    val toTranslationArray = udf((s: String) => parseSimpleEnglish(s))
    val kanjiAlive = spark.read.json(ScalaConfig.KanjiAliveP).withColumnRenamed("kanji", "kaKanji")
      .withColumnRenamed("kmeaning", "kaMeanings")
      .withColumnRenamed("onyomi", "kaOnYomi")
      .withColumnRenamed("kunyomi", "kaKunYomi")
      .withColumnRenamed("onyomi_ja", "kaOnYomi_ja")
      .withColumnRenamed("kunyomi_ja", "kaKunYomi_ja")


    kanjiAlive.show(8)
    println("Number of kanjiAlive: " + kanjiAlive.count()) //expensive


    val tanosKanji = spark.read.json(ScalaConfig.KanjiTanosPFreq).withColumnRenamed("Kanji", "tanosKanji")
      .withColumnRenamed("jlpt", "tanosJlpt")
      .withColumnRenamed("Kunyomi", "tanosKunyomi")
      .withColumnRenamed("Onyomi", "tanosOnyomi")
      .withColumnRenamed("English", "tanosMeaning")


    tanosKanji.show(9)

    def combineAllMeanings(meanings: Seq[Row], tanosMeaning: Seq[(String, String)], kaMeanings: Seq[(String, String)]): Seq[(String, String)] = {
      val ms = if (meanings != null) meanings.map { case Row(x: String, y: String) => (x, y); case _ => ("", "") } else Seq[(String, String)]()
      val tns = if (tanosMeaning != null) tanosMeaning else Seq[(String, String)]()
      val kans = if (kaMeanings != null) kaMeanings else Seq[(String, String)]() //toSet
      (ms ++ tns ++ kans).toSet.toSeq
    }

    val toCombinedMeaningsSet = udf((meanings: Seq[Row], tanosMeaning: Seq[(String, String)], kaMeanings: Seq[(String, String)]) => combineAllMeanings(meanings, tanosMeaning, kaMeanings))

    //println("kd meanings count: " + kanjidic.filter(r => getFld(r, "kdMeanings") != "").count)
    val combinedMeanings = kanjidic
      .join(kanjiAlive, kanjidic("literal") === kanjiAlive("kaKanji"), "fullouter")
      .join(tanosKanji, kanjidic("literal") === tanosKanji("tanosKanji"), "fullouter")
      .withColumn("meanings", toCombinedMeaningsSet('kdMeanings, toTranslationArray('tanosMeaning), toTranslationArray('kaMeanings)))
      .select('literal, 'meanings)
      .withColumnRenamed("literal", "cmLiteral")
      .alias("combinedMeanings")
    combinedMeanings.show(9)


    val wikiRadicals = spark.read.json(ScalaConfig.WikiRadsDP)

    val tokenizerCache = new Tokenizer()
    println("Now calculating vocabulary and whatnot")
    //def doTransliteration(japanese: String):KanaTransliteration = KanaTransliteration(japanese,japanese.toHiragana(tokenizerCache), japanese.toKatakana(tokenizerCache),japanese.toRomaji(tokenizerCache))

    val uBaseForm = udf((word: String) => word.tokenize().headOption.map(_.getBaseForm()).getOrElse(""))

    val uFurigana = udf((word: String) => word.furigana().map(f => (f.original, f.kana.hiragana)))
    val uTransliterate = udf((japanese: String) => KanaTransliteration(japanese): KanaTransliteration)
    val uTransliterateA = udf((js: Seq[String]) => js.map(japanese => KanaTransliteration(japanese): KanaTransliteration))
    val uSum3 = udf((a: Int, b: Int, c: Int) => a + b + c)

    val rawVocabulary = spark.read.json(ScalaConfig.FrequentWordsP)
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
      .cache()
    vocabulary.show(500)

    val uExtractKanji = udf((r: Row) => r match {
      case Row(x: String, y: String) => x: String
      case _ => ""
    })
    val flatten = udf((xs: Seq[Seq[(String, String)]]) => xs.flatten)
    val kanjiReadings = vocabulary.select('word, 'totalOcurrences, explode('furigana) as "furigana")
      .groupBy('furigana)
      .agg(sum('totalOcurrences) as "Occ")
      .orderBy('furigana)
      .withColumn("k", uExtractKanji('furigana))
      .groupBy('k)
      .agg(collect_list(struct('furigana, 'Occ)) as "readingsWFreq") //if doesn't work remove struct

    kanjiReadings.show(31, false)

    val tatoes = spark.read.json(ScalaConfig.TatoebaDP)

    //--- Errors ---
    //val radicals = spark.read.json(ScalaConfig.KanjiAliveRadicalP) //radical isn't properly encoded in file it seems //EN EL ARCHIVO ORIGINAL POR ESO
    //val kanjiVG = spark.read.json(ScalaConfig.KanjiVGDP) //returns empty

    def parseKunToArray(k: String): Array[String] = if (k != null && k.trim != "") k.split("、") else Array[String]()

    def parseOnToArray(o: String): Array[String] = if (o != null && o.trim != "") o.split("、") else Array[String]()

    def parseKDToArray(rs: Seq[Row]) = rs.flatMap {
      case Row(x: String, y: String) if (x == "ja_on" || x == "ja_kun" && y != "") => (Some(y.toHiragana().split("。").head)) // .replace("-", "") //ignores endings & positions in kanjidic
      case _ => None
    }

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

    def cleanUpR(rs: Seq[String]): Seq[String] = rs.map(_.replace("-", "")).toSet.toSeq

    val mapKDReadings = udf((rs: Seq[Row], k: String, y: String, tk: String, to: String) => {
      val kunYomi = mapKDReadingsKun(rs, k, tk)
      val onYomi = mapKDReadingsOn(rs, y, to)
      cleanUpR((kunYomi ++ onYomi).map(_.toHiragana())) //.toSet.toSeq
    }
    )

    // -- Reading joins --
    //MUST REMOVE DUPLICATE READINGS
    val readingsDF = lvlsRaw.join(kanjidic, lvlsRaw("kanji") === kanjidic("literal"), "left")
      .join(kanjiAlive, lvlsRaw("kanji") === kanjiAlive("kaKanji"), "left")
      .join(tanosKanji, lvlsRaw("kanji") === tanosKanji("tanosKanji"), "left") //not taken into consideration yet
      //.select('kanji as "readingsKanji", mapKDReadings('kdReadings, 'kaKunYomi_ja, 'kaOnYomi_ja) as "readings", uTransliterateA(uMapKDReadingsKun('kdReadings, 'kaKunYomi_ja)) as "kunYomi", uTransliterateA(uMapKDReadingsOn('kdReadings, 'kaOnYomi_ja)) as "onYomi")
      .select('kanji as "readingsKanji", mapKDReadings('kdReadings, 'kaKunYomi_ja, 'kaOnYomi_ja, 'tanosKunyomi, 'tanosOnyomi) as "readings", uTransliterateA(uMapKDReadingsKun('kdReadings, 'kaKunYomi_ja, 'tanosKunyomi)) as "kunYomi", uTransliterateA(uMapKDReadingsOn('kdReadings, 'kaOnYomi_ja, 'tanosOnyomi)) as "onYomi")
    //.select('readingsKanji, 'readings, uTransliterateA('kunYomi) as "kunYomi", uTransliterateA('onYomi) as "onYomi")


    readingsDF.show(21)

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
      .drop(col("isEUCJP"))
      .drop(col("isKANGXI"))
      .drop(col("isKanji"))
      .drop(col("literal"))
      .drop(col("processedRadicals"))
      .drop(col("fKanji"))
      .drop(col("tanosKanji"))
      .drop(col("kaKanji"))
      .drop(col("cKanji"))
      .drop(col("freqKanji"))
      .drop(col("twKanji"))
      .drop(col("wkKanji"))
      .drop(col("newsKanji"))
      .drop(col("ffragments"))
      .drop(col("tanosJlpt"))
      .drop(col("kdJlpt"))
      .drop(col("tanosKunyomi"))
      .drop(col("tanosOnyomi"))
      .drop(col("kgrade"))
      .drop(col("kstroke"))
      .drop('kdMeanings) //drops redundant meanings columns
      .drop('tanosMeaning)
      .drop('kaMeanings)
      .drop("cmLiteral")
      .drop('readingsKanji) //drops redundant readings columns
      .drop(col("kdReadings"))
      .drop(col("kaKunYomi_ja"))
      .drop(col("kaOnYomi_ja"))
      .drop(col("kaKunYomi"))
      .drop(col("kaOnYomi"))
      .drop('readings)
      .drop('k)
      .drop('kdFreq)
    //.orderBy(col("jlpt")) //can't resolve
    jointDF.show(23)

    val kanjis = jointDF.drop(col("dic_numbers"))
      .drop(col("query_codes"))
      .orderBy(col("rank"))

    kanjis.show(50)

    println("TrimmedDF Count: " + kanjis.count()) //expensive

    ///////IMPORTANT:------- UDF MUST NOT THROW ANY INTERNAL EXCEPTIONS; THAT INCLUDES NULL OR THEY WONT WORK---------
    readingsDF.show(20)
    println("Number of readings: " + readingsDF.count()) //expensive

    //PROBLEM after flatmaping jcommas
    val uMkStr = udf((a: Seq[String]) => a.mkString(","))

    //Describes schemas (expensive?)
    kanjis.cache().printSchema()
    vocabulary.cache().printSchema()

    //Writes to File
    //Writes Readings
    readingsDF.select('readingsKanji, uMkStr('readings) as "readings").coalesce(1).write.mode(SaveMode.Overwrite).csv("outputSF") //readings.csv

    (vocabulary, kanjis)
  }

    //START REFACTORING CODE

    //END REFACTORING CODE

    //Parse the thing
    println(ScalaConfig.vocabCacheFN)
    println(ScalaConfig.kanjiCacheFN)
    val (vocabulary:DataFrame, kanjis:DataFrame) = (read(ScalaConfig.vocabCacheFN), read(ScalaConfig.kanjiCacheFN)) match {
      case (Success(vocab), Success(kanjis)) => (vocab, kanjis)
      case _ => parseAll
    }

    kanjis.show(50)
//    kanjis.printSchema()
    vocabulary.show(50)
//    vocabulary.printSchema()

    //Joinning vocabs with kanji
    def containsKanjiFilter(r: Row): Boolean = filterWord(r).containsKanji
    val uExtractKanjiFromVocab = udf((word:String) => word.extractUniqueKanji.map(_.toString).toSeq)

    //Maps to dataset
//    println("Printing vocabulary DS")
//    val vocabsds = vocabulary.as[VocabularyDS]
//    vocabsds.printSchema()
//    vocabsds.show(50)

    println("joining vocabs with kanji")

    val vocabPerKanji: Dataset[Row] = vocabulary.filter(r => containsKanjiFilter(r)) //.filter(r => containsJoyoKFilter(r)) //not worth
      .withColumn("vocabKanji", uExtractKanjiFromVocab('word))
      .withColumn("vocabZipped", struct(vocabulary.columns.head, vocabulary.columns.tail: _*))
      .select('word, explode('vocabKanji) as "vocabK", 'vocabZipped)
      .groupBy('vocabK)
      .agg(collect_list('vocabZipped) as "vocabsPerKanji")

    vocabPerKanji.show(100)

    val jointKV = kanjis
      .join(vocabPerKanji, kanjis("kanji") === vocabPerKanji("vocabK"), "left")
      .drop('vocabK)

    jointKV.show(50)

    val kanjiPerVocab: Dataset[Row] = vocabulary.filter(r => containsKanjiFilter(r))
      .withColumn("vocabKanji", uExtractKanjiFromVocab('word))
      .select('*, explode('vocabKanji) as "vocabK")
      .join(kanjis.withColumn("kanjiZipped", struct(kanjis.columns.head, kanjis.columns.tail: _*)).select('kanji, 'kanjiZipped), 'vocabK === kanjis("kanji"), "left")
      .drop('vocabKanji)
      .drop('vocabK)
      .groupBy('word)
      .agg(collect_list('kanjiZipped) as "kanjisInVocab")
      .withColumnRenamed("word", "wordK")

    kanjiPerVocab.show(49, false)

    val jointVK = vocabulary.join(kanjiPerVocab, kanjiPerVocab("wordK") === vocabulary("word")).drop('wordK)
    jointVK.show(48, false)
    //Now join to vocab with an alias.

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


object ScalaConfig {


  //ALERT __________------- ---  JSON MUST BE IN COMPACT FORMAT FOR SPARK TO READ
  private val standardPath = "./utils/"
  private val oldPath = "/run/media/dsalvio/Media/Development/Projects/Java/Full-Out/KPE/Java/"

  val outputCache = "./outputCache/"
  val kanjiCache = outputCache + "kanjiCache"
  val vocabCache = outputCache + "vocabCache"

  val vocabCacheFN = vocabCache + "/vocabCached.snappy.parquet"
  val kanjiCacheFN = kanjiCache + "/kanjiCached.snappy.parquet"

  val Edict: String =  standardPath + "edict-utf-8"
  val levelsPath = standardPath + "jlpt-levels.json"
  val kanjidicPath = standardPath + "kanjidic2-compact.json"
  val KanjiAliveP = standardPath + "ka_data-compact.json"
  val KanjiTanosPFreq = standardPath + "tanos-jlpt-compact.json"

  val FrequentWordsP = standardPath + "word-frequency-descriptive-compact.json"
  val WikiRadsDP = standardPath + "japanese-radicals-wikipedia-adapted-compact.json"
  val TatoebaDP = standardPath + "tatoeba-compact.json"
  val JmDicP = standardPath + "jmdic-compressed.json" //-compact

  val KanjiVGDP = standardPath + "kanjivg-compact.json"
  val KradFN = standardPath + "kradfile-u-clean"

  val aoFreq = standardPath + "aozora.csv" //|all|51479326||   1|
  val twitterFreq = standardPath + "twitter.csv"
  val newsFreq = standardPath + "news.csv" //"all",10318554,1
  val wikipediaFreq = standardPath + "wikipedia.csv"

  val allFreqs =  standardPath + "all-freqs"

  val KanjiAliveRadicalP = standardPath + "japanese-radicals-compact.json"
  val CompositionsPath = standardPath + "cjk-decomp-0.4.0.txt"
}