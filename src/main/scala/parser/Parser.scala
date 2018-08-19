package parser

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{length, trim, when}

import models._
import sjt.JapaneseSyntax._
import sjt.JapaneseInstances._

//TODO: Export to Elasticsearch

//TODO: Add resource files to build
//TODO: Add more info to the vocab including: rankOfKanjis(?)
//TODO: Get recursive components for kanjis with their ranks and the rank of their readings
//TODO: Fix radical column
//TODO: Include tanos when parsing dic readings

object Parser {
  //val logFile = "/opt/spark-2.1.0-bin-hadoop2.7/README.md" // Should be some file on your system
  implicit val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
  val conf = new SparkConf().setMaster("local[*]").setAppName("Simple Application")

  import spark.implicits._

  //To reduce spark output
  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def extractVocabsForKanji(vocabulary:DataFrame): DataFrame = {
    def getFld(r: Row, name: String) = r.getString(r.fieldIndex(name))
    def filterWord(r: Row): String = getFld(r, "word")

    def containsKanjiFilter(r: Row): Boolean = filterWord(r).containsKanji
    val uExtractKanjiFromVocab = udf((word:String) => word.extractUniqueKanji.map(_.toString).toSeq)

    val vocabPerKanji: Dataset[Row] = vocabulary.filter(r => containsKanjiFilter(r))
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
  def printInfo(df: DataFrame, name: String = "")(numberToShow: Int = 50, count: Boolean = true, schema: Boolean = false, truncateColumns: Boolean = true) = {
    if (count) println(s"$name DF has a total of ${df.count()} rows.")
    if (numberToShow > 0){
      println(s"$name DF, first $numberToShow rows:")
      df.show(numberToShow, truncateColumns)
    }
    if (schema) {
      println(s"$name DF Schema:")
      df.printSchema()
    }
  }

  def main(args: Array[String]): Unit = {
    parseAll
    spark.stop
  }

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
    printInfo(kanjiAlive, "KanjiAlive")(schema = true, truncateColumns = false)

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
    printInfo(combinedMeanings, "Meanings")(truncateColumns = false)

    val vocabulary = LocalCache.of(Config.vocabPath, VocabularyParser.parseVocabulary(Config.FrequentWordsP, edict), true).cache()
    printInfo(vocabulary, "Vocabulary")(100)

    val inferedReadings = LocalCache.of(Config.inferedReadings, ReadingParser.inferReadingsFromVocab(vocabulary), true)
    printInfo(inferedReadings, "KanjiReadings")()

    val dicReadings = LocalCache.of(Config.dicReadings, ReadingParser.parseReadingsFromDictionaries(lvlsRaw,kanjidic, kanjiAlive, tanosKanji), true)
    printInfo(dicReadings, "dicReadings")()

    val readings = LocalCache.of(Config.allReadings, ReadingParser.combineInferedReadingsWithDicReadings(inferedReadings, dicReadings), true)
    printInfo(readings, "Readings")()

    // --- Final Data Joins ---
    val rawJointDF = lvlsRaw.alias("levelRaw").join(kanjidic, lvlsRaw("kanji") === kanjidic("literal"), "left")
      .join(allFragmentsLists, lvlsRaw("kanji") === allFragmentsLists("fKanji"), "left")
      .join(tanosKanji, lvlsRaw("kanji") === tanosKanji("tanosKanji"), "left")
      .join(kanjiAlive, lvlsRaw("kanji") === kanjiAlive("kaKanji"), "left")
      .join(comps, lvlsRaw("kanji") === comps("cKanji"), "left")
      .join(kanjiFreqs, lvlsRaw("kanji") === kanjiFreqs("freqKanji"), "left")
      .join(combinedMeanings, lvlsRaw("kanji") === combinedMeanings("cmLiteral"), "left") //.join(combinedMeanings, col("levelRaw.kanji") === col("combinedMeanings.cmLiteral"), "left")
      .join(inferedReadings, lvlsRaw("kanji") === inferedReadings("k"))
      .join(readings, col("levelRaw.kanji") === readings("readingsKanji"), "left")
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
      .drop('kdMeanings).drop('tanosMeaning).drop('kaMeanings) //drops redundant meanings columns
      .drop("cmLiteral")
      .drop('readingsKanji) //drops redundant readings columns
      .drop(col("kdReadings")).drop('readings)
      .drop(col("kaKunYomi_ja")).drop(col("kaOnYomi_ja")).drop(col("kaKunYomi")).drop(col("kaOnYomi"))
      .drop('k)
      .drop('kdFreq)
      .drop('readingsWFreq)
    //.orderBy(col("jlpt")) //can't resolve
    jointDF.show(23)

    val kanjis = jointDF.drop(col("dic_numbers"))
      .drop(col("query_codes"))
      .orderBy(col("rank"))

    printInfo(kanjis, "Kanjis")(50, true, true)
    printInfo(vocabulary, "Vocabulary")(50, true, true)

    readings.show(20)
    println("Number of readings: " + readings.count()) //expensive

    println("-- joining vocabs <-> kanji-- ")
    val vocabPerKanji = extractVocabsForKanji(vocabulary)

    val jointKV = kanjis.join(vocabPerKanji, kanjis("kanji") === vocabPerKanji("vocabK"), "left").drop('vocabK)
    jointKV.show(50)

    val kanjiPerVocab = extractKanjiPerVocab(vocabulary, kanjis)
    kanjiPerVocab.show(49, false)

    val jointVK = vocabulary.join(kanjiPerVocab, kanjiPerVocab("wordK") === vocabulary("word")).drop('wordK).cache()
    jointVK.show(48, false)

//    def jsonToAwsEntry(word:String, json:String):String = {
//      "{\"PutRequest\": {\"Item\": {\"word\": {\"S\": \"" + word + "\"},\"data\": {\"S\": \"" + json.replaceAll("\"", "\\\\\"") + "\"}}}}"
//    }
//    val ujsonToAwsEntry = udf((word:String, json:String) => jsonToAwsEntry(word, json))
//
//    import org.apache.spark.sql.functions.to_json
//    val awsEntries = jointVK.withColumn("allZipped", struct(jointVK.columns.head, jointVK.columns.tail: _*))
//        .select(ujsonToAwsEntry('word, to_json('allZipped).as('json)).as('awsEntry))

    val awsEntries = AwsJsonEntryConverter.convertToAwsJsonEntry(jointVK)
    awsEntries.show(48, false)
    awsEntries.repartition(600).write.mode(SaveMode.Overwrite).text("output-aws-entries")


    val topVocabulary = vocabulary.filter('totalOcurrences.gt(1000)) //2000 = 15k words, 1000 = 24k words, 750 = 28k words, 500 = 34k words, 100 = 75k words, 50 = 99k words, 0 = 299k words
    val topVK = jointVK.filter('totalOcurrences.gt(1000))

    println("---> End of transformations <---")

//    // --> Writes to File <--
//    //Writes Kanji
//    //Writes Kanji (default partitioning)
//    kanjis.write.mode(SaveMode.Overwrite).json("output-kanji-default")
//    //Writes Kanji (single partition)
//    kanjis.coalesce(1).write.mode(SaveMode.Overwrite).json("output-kanji-single")
//    //Writes Kanji (one partition per entry)
//    kanjis.repartition(kanjis.count().toInt).write.mode(SaveMode.Overwrite).json("output-kanji-individual")
//
//    //Writes Kanji with Vocab
//    //Writes Joint Vocab with Kanji (default partitioning)
//    jointKV.write.mode(SaveMode.Overwrite).json("output-kanji-with-vocab-default")
//    //Writes Joint Vocab with Kanji (single partition)
//    jointKV.coalesce(1).write.mode(SaveMode.Overwrite).json("output-kanji-with-vocab-single")
//    //Writes Joint Vocab with Kanji (one partition per entry)
//    jointKV.repartition(jointKV.count().toInt).write.mode(SaveMode.Overwrite).json("output-kanji-with-vocab-individual")
//
//    //Writes Vocabulary
//    //Writes Vocabulary (default partitioning)
//    vocabulary.repartition(600).write.mode(SaveMode.Overwrite).json("output-vocab-default")
//    //Writes Vocabulary (single partition)
//    vocabulary.coalesce(1).write.mode(SaveMode.Overwrite).json("output-vocab-single")
//    //Writes Vocabulary (one partition per entry). Error: produces 360k files.
////    vocabulary.repartition(vocabulary.count().toInt).write.mode(SaveMode.Overwrite).json("output-vocab-individual")

      //--Writes Top Vocabulary
//      topVocabulary.repartition(600).write.mode(SaveMode.Overwrite).json("output-top-vocab-default")
//
//    //Writes Vocab with Kanji
//    //Writes Joint Vocab with Kanji (default partitioning)
//    jointVK.write.mode(SaveMode.Overwrite).json("output-vocab-with-kanji-default")
//    //Writes Joint Vocab with Kanji (single partition)
//    jointVK.coalesce(1).write.mode(SaveMode.Overwrite).json("output-vocab-with-kanji-single")

    //--Writes Top Vocabulary with Kanji
//    topVK.repartition(600).write.mode(SaveMode.Overwrite).json("output-top-vocab-default")

    //--Writes Aws Entries

  }
}
