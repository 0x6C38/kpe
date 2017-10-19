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