package parser

object Config {
  //ALERT!!! JSON MUST BE IN COMPACT FORMAT FOR SPARK TO READ
  private val standardPath = "./utils/"
  private val oldPath = "/run/media/dsalvio/Media/Development/Projects/Java/Full-Out/KPE/Java/"

  val vocabPath = standardPath + "vocab"

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

  val mCombinedP = standardPath + "combined-meanings"
}