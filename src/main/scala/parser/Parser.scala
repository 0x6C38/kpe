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

//TODO: Link summarized meanings with result DF
//TODO: Use the correct column to extract the readings for kanjis since only 1.2k or so are being parsed

//TODO: Add resource files to build
//TODO: Add more info to the vocabs including: MeaningInEnglish + kuromojiTokens(?) + rankOfKanjis(?)
//TODO: Get recursive components for kanjis
//TODO: Add ranks of components for kanjis
//TODO: Add ranks of readings for kanjis
//TODO: Fix radical column
//TODO: Write final vocab to file
//TODO: Write final parse into Kanji class
//TODO: Export kanjis

object Hello {
  val conf = new SparkConf().setMaster("local[*]").setAppName("Simple Application")
  val spark:SparkSession = SparkSession.builder.master("local").getOrCreate
  import spark.implicits._ //necesary import

  //To reduce spark output

  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)


  def extractKanjiFromVocabulary(vs:Dataset[Row]):Array[(FrequentWordRawParse, Array[Char])] ={
    val vParsed:Array[FrequentWordRawParse] = vs.as[FrequentWordRawParse].collect()
    val kanjiPerVocab:Array[(FrequentWordRawParse, Array[Char])] = vParsed.map((w:FrequentWordRawParse) => (w, w.word.extractKanji.toCharArray))
    kanjiPerVocab
  }
  val extractKanjiFromVocab = udf((word:String) => word.extractKanji.map(_.toString))

  //val avgRankings = udf( (first: String, second: String, third:String, fourth:String) =>  (first.toInt + second.toInt + third.toInt  + fourth.toInt).toDouble / 4 )
  /*def extractVocabularyForKanji(vs:Array[(FrequentWordRawParse, List[Char])]):Array[(Char, Array[FrequentWordRawParse])] = {}*/

  def main(args: Array[String]): Unit = {
    //val logFile = "/opt/spark-2.1.0-bin-hadoop2.7/README.md" // Should be some file on your system
    //val conf = new SparkConf().setMaster("local[*]").setAppName("Simple Application")

    //--- Kanji Frequency ---
    val toDbl    = udf[Double, String]( _.toDouble)

    val aofreqs = spark.read.option("inferSchema", "true").csv(ScalaConfig.aoFreq)
      .withColumnRenamed("_c0", "freqKanji")
      .withColumnRenamed("_c1", "aoOcu")
      .withColumnRenamed("_c2", "aoFreq")
      .withColumn("aoRank", dense_rank().over(Window.orderBy(col("aoOcu").desc)))

    //aofreqs.show(5)

    val twitterFreqs = spark.read.option("inferSchema", "true").csv(ScalaConfig.twitterFreq)
      .withColumnRenamed("_c0", "twKanji")
      .withColumnRenamed("_c1", "twOcu")
      .withColumnRenamed("_c2", "twFreq")
      .withColumn("twRank", dense_rank().over(Window.orderBy(col("twOcu").desc)))

    val wikipediaFreqs = spark.read.option("inferSchema", "true").csv(ScalaConfig.wikipediaFreq)
      .withColumnRenamed("_c0", "wkKanji")
      .withColumnRenamed("_c1", "wkOcu")
      .withColumnRenamed("_c2", "wkFreq")
      .withColumn("wkRank", dense_rank().over(Window.orderBy(col("wkOcu").desc)))

    val newsFreqs = spark.read.option("inferSchema", "true").csv(ScalaConfig.newsFreq)
      .withColumnRenamed("_c0", "newsKanji")
      .withColumnRenamed("_c1", "newsOcu")
      .withColumnRenamed("_c2", "newsFreq")
      .withColumn("newsRank", dense_rank().over(Window.orderBy(col("newsOcu").desc)))

    val avgRankings = udf( (first: String, second: String, third:String, fourth:String) =>  (first.toInt + second.toInt + third.toInt  + fourth.toInt).toDouble / 4 )

    val kanjiFreqs = aofreqs.join(twitterFreqs, aofreqs("freqKanji") === twitterFreqs("twKanji"))
      .join(wikipediaFreqs, aofreqs("freqKanji") === wikipediaFreqs("wkKanji"))
      .join(newsFreqs, aofreqs("freqKanji") === newsFreqs("newsKanji"))
      .withColumn("avgRank", avgRankings(col("newsRank"), col("wkRank"),col("twRank"),col("aoRank")))
      .withColumn("rank", dense_rank().over(Window.orderBy(col("avgRank").asc)))
    //kanjiFreqs.show(15)

    //--- Kanji Composition ---
    val rawComps = spark.read.textFile(ScalaConfig.CompositionsPath).filter(l => l.startsWith(l.head + ":") && l.head.isKanji)
    val comps = rawComps.map(l => l.head.toString -> Composition.parseKCompLine(l))
      .withColumnRenamed("_1", "cKanji")
      .withColumnRenamed("_2", "components")
    //val fivecomps = comps.take(50)

    val lvlsRaw = spark.read.json(ScalaConfig.levelsPath)
    val lvls = lvlsRaw.as[KanjiLevel].collect()

    val kanjidic = spark.read.json(ScalaConfig.kanjidicPath) //cannot resolve 'UDF(meanings)' due to data type mismatch: argument 1 requires string type, however, '`meanings`' is of array<struct<m_lang:string,meaning:string>> type.;;
      .withColumnRenamed("jlpt", "kdJlpt")
      .withColumnRenamed("meanings", "kdMeanings").cache()
    kanjidic.show(7)


    val allFragmentsLists = spark.read.option("delimiter", ":").format("csv").load(ScalaConfig.KradFN)
      .withColumnRenamed("_c0", "fKanji").withColumnRenamed("_c1", "ffragments")
      .withColumn("fKanji", trim(col("fKanji"))).withColumn("ffragments", trim(col("ffragments"))) //must trim to match


    def parseSimpleEnglish(s:String):Seq[(String, String)]  = if (s != null) s.trim.split(", ").map(t => ("en", t)) else Seq[(String,String)]()
    val toTranslationArray = udf((s:String) => parseSimpleEnglish(s))
    val kanjiAlive = spark.read.json(ScalaConfig.KanjiAliveP).withColumnRenamed("kanji", "kaKanji")
      .withColumnRenamed("kmeaning", "kaMeanings")
    kanjiAlive.show(8)

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
    val toCombinedMeaningsSet = udf((meanings:Seq[Row], tanosMeaning:Seq[(String,String)], kaMeanings:Seq[(String,String)]) => combineAllMeanings(meanings, tanosMeaning, kaMeanings))
    val combinedMeanings = kanjidic
      .join(kanjiAlive, kanjidic("literal") === kanjiAlive("kaKanji"), "left")
      .join(tanosKanji, kanjidic("literal") === tanosKanji("tanosKanji"), "left")
      .withColumn("meanings", toCombinedMeaningsSet('kdMeanings, toTranslationArray('tanosMeaning), toTranslationArray('kaMeanings)))
      .select('literal, 'meanings)
      //.drop('kdMeanings)
      //.drop('tanosMeaning)
      //.drop('kaMeanings)
    combinedMeanings.show(9)


    val wikiRadicals = spark.read.json(ScalaConfig.WikiRadsDP)

    val tokenizerCache = new Tokenizer()

    //def doTransliteration(japanese: String):KanaTransliteration = KanaTransliteration(japanese,japanese.toHiragana(tokenizerCache), japanese.toKatakana(tokenizerCache),japanese.toRomaji(tokenizerCache))
    val transliterate = udf((japanese: String) =>  KanaTransliteration(japanese):KanaTransliteration)
    val vocabulary = spark.read.json(ScalaConfig.FrequentWordsP)
      .withColumn("internetRank", dense_rank().over(Window.orderBy(col("internetRelative").desc)))
      .withColumn("novelsRank", dense_rank().over(Window.orderBy(col("novelRelative").desc)))
      .withColumn("subtitlesRank", dense_rank().over(Window.orderBy(col("subtitlesRelative").desc)))
      .withColumn("rank", dense_rank().over(Window.orderBy(col("averageRelative").desc)))
      .withColumn("transliterations", (transliterate(col("word"))))

    vocabulary.show(25)
    val tatoes = spark.read.json(ScalaConfig.TatoebaDP)

    //--- Errors ---
    //val radicals = spark.read.json(ScalaConfig.KanjiAliveRadicalP) //radical isn't properly encoded in file it seems //EN EL ARCHIVO ORIGINAL POR ESO
    //val kanjiVG = spark.read.json(ScalaConfig.KanjiVGDP) //returns empty

    // --- Vocabulary ---
    def getFld(r:Row, name:String) = r.getString(r.fieldIndex(name))
    def filterWord(r:Row):String = getFld(r, "word")

    def containsKanjiFilter(r: Row): Boolean = filterWord(r).containsKanji

    val vocabularyWK: Dataset[Row] = vocabulary.filter(r => containsKanjiFilter(r)) //.filter(r => containsJoyoKFilter(r)) //not worth

    //val kanjiPerVocab = extractKanjiFromVocabulary(vocabularyWK)//* REVERSE CHANGES AND MAKE USE OF THIS
    //val kanjiPerVocab = vocabularyWK.withColumn("kanjis", extractKanjiFromVocab(col("word").as[List[String]]))
    //kanjiPerVocab.show(60)

    //val vocabPerKanji: Array[(String, Array[FrequentWordRawParse])] = lvls.take(50).map(l => (l.kanji -> kanjiPerVocab.filter(kpv => kpv._2.contains(l.kanji.trim.head)).map(e => e._1)))//*
    //vocabPerKanji.foreach(v => println(col("_1") + ":" + col("_2").toString()))//*
    /*
    val ts = kanjiPerVocab.dtypes
    val filterCA = udf((k:String, c: mutable.WrappedArray[String]) => (c.contains(k)):Boolean)

    val listContainsK = udf((k:String, c: mutable.WrappedArray[String]) => (c.contains(k)):Boolean)

    //val findVocabForKanji = udf((kanji:String) => kanjiPerVocab.where(array_contains(col("kanjis"), kanji)).collect().map)// r => filterCA(kanji, col("kanjis")))) // .getAs[List[String]]("kanjis").contains(kanji))

    val vocabPerKanji = lvlsRaw.withColumn("vocabulary", findVocabForKanji(col("kanji")))
    //println(kanjiPerVocab.count())
    val vocabPerKanji = kanjiPerVocab.joinWith(lvlsRaw, listContainsK(lvlsRaw("kanji"), kanjiPerVocab("kanjis"))).orderBy(col("_2")) //array_contains(kanjiPerVocab("kanjis"), lvlsRaw("kanji")))
    //vocabPerKanji.groupBy(col("_2"))
    vocabPerKanji.show(16)
    println(vocabPerKanji.count())
  */

    //val vocabSpark = spark.sparkContext.parallelize(vocabPerKanji).toDS()
    //val ts = vocabSpark.dtypes
    //val vocabPerKanji:Array[(Char, Array[FrequentWordRawParse])] = extractVocabularyForKanji(kanjiPerVocab)
    //vocabPerKanji.foreach(v => println(v._1 + ":" + v._2.map(_.word).mkString(",")))
    //vocabPerKanji.foreach(v => println(col("kanji") + ":" + col("vocabulary").toString()))

    //val vocabSpark = spark.createDataset(vocabPerKanji)//*
    //val vocabSpark = vocabPerKanji

    val translationsDictionary = spark.read.json(ScalaConfig.JmDicP) //incorrect formatting

    // --- Final Data Joins ---
    val rawJointDF = lvlsRaw.join(kanjidic, lvlsRaw("kanji") === kanjidic("literal"), "left")
      .join(allFragmentsLists, lvlsRaw("kanji") === allFragmentsLists("fKanji"), "left")
      .join(tanosKanji, lvlsRaw("kanji") === tanosKanji("tanosKanji"), "left")
      .join(kanjiAlive, lvlsRaw("kanji") === kanjiAlive("kaKanji"), "left")
      .join(comps,      lvlsRaw("kanji") === comps("cKanji"),     "left")
      .join(kanjiFreqs, lvlsRaw("kanji") === kanjiFreqs("freqKanji"),"left")
      .cache
      //.join(vocabSpark, lvlsRaw("kanji") === vocabSpark("_1"),"left") //Correct _1 name //*

    rawJointDF.show()
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
    //.orderBy(col("jlpt")) //can't resolve

    jointDF.show()

    val trimmedDF = jointDF.drop(col("dic_numbers"))
      .drop(col("query_codes"))
      .drop(col("readings"))
      .orderBy(col("rank"))

    trimmedDF.show(50)


    ///////IMPORTANT:------- UDF MUST NOT THROW ANY INTERNAL EXCEPTIONS; THAT INCLUDES NULL OR THEY WONT WORK---------
    val mapReadingsUDF = udf((k:String, y:String) => (if (k != null && k.trim != "") k.toHiragana() + "、" else "")  + (if (y != null && y.trim != "") y.toHiragana() else ""))

    val readings = trimmedDF.select('kanji, mapReadingsUDF('kunyomi_ja, 'onyomi_ja) as "readings").filter(r => getFld(r, "readings") != "")

    readings.show(20)

    //readings.coalesce(1).write.csv("outputSF") //readings.csv

//    trimmedDF.write.json("output")
    //trimmedDF.coalesce(1).write.json("outputSF")

    spark.stop
  }
}


object ScalaConfig{
  //ALERT __________------- ---  JSON MUST BE IN COMPACT FORMAT FOR SPARK TO READ
  private val standardPath = "./utils/"
  private val oldPath = "/run/media/dsalvio/Media/Development/Projects/Java/Full-Out/KPE/Java/"
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

  val KanjiAliveRadicalP = standardPath + "japanese-radicals-compact.json"
  val CompositionsPath = standardPath +"cjk-decomp-0.4.0.txt"
}