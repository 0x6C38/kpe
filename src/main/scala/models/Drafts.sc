import com.atilika.kuromoji.ipadic.Tokenizer
import models._
import sjt.JapaneseInstances._
import sjt.JapaneseSyntax._


//TODO: Add extractHiragana + extractKatakana + extractKana methods to SJT
//TODO: Add extractKanjiReading (?)

val tokenizerCache = new Tokenizer()
def getReadingInSingleKanjiStr(s:String, kanji:String, possibleKunReadings:String, possibleOnReadings:String, actualReading:String) = {
  val possibleReadings = possibleKunReadings.split('、') ++ possibleOnReadings.split('、').map(_.toHiragana(tokenizerCache))
  val kanjisInStr = s.extractKanji
}
val exampleStr1 = "一日"
val exampleStr2 = "行きます"
val exampleStr3 = "聞きます"

"いきます" diff "きます"
"いきますい".replaceFirst("きます", "")


//"[[en,file], [en,russian], [en,something]]".tail.init.split(", ").map(e => e.trim.tail.init.split(",")).map(f => Translation(f.head, f(1)))
val sample1 = "[[en,file], [en,russian], [en,something]]"
models.Utils.parseCMeanings(sample1)

def parseSimpleEnglish(s:String):Array[TEntry]  = {
  s.trim.split(", ").map(t => TEntry("en", t))
}
val sample2 = "day, sun, Japan"
val sample3 = "day"

parseSimpleEnglish(sample2)
parseSimpleEnglish(sample3)

def parseAllMeanings(meanings:String, English:String, kmeaning:String):Set[TEntry] = {
  Utils.parseCMeanings(meanings) ++ parseSimpleEnglish(English) ++ parseSimpleEnglish(kmeaning) toSet
}