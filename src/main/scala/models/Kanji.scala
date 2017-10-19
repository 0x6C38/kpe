package models

import com.atilika.kuromoji.ipadic.Tokenizer
import sjt.JapaneseInstances._
import sjt.JapaneseSyntax._
//import bayio.kpe.{Frequency, Word}
case class KanjiLevel(kanji:String, level:BigInt)

case class Meaning(language:String, meaning:String)
case class KunYomi(hiragana:String, romaji:String)
case class OnYomi(katakana:String, romaji:String)

case class KanaTransliteration(original:String, katakana:String, hiragana:String, romaji:String) extends Serializable
object KanaTransliteration{
  val tokenizerCache = new Tokenizer()
  def apply(s:String) = {
    new KanaTransliteration(s, s.toHiragana(tokenizerCache), s.toKatakana(tokenizerCache), s.toRomaji(tokenizerCache))
  }
}
case class Translation(english:String, japanese:String, kanaTranslation: KanaTransliteration)
case class Word(translation:Translation, frequency:WordFrequency)

case class Frequency(relative:Double, ocurrences:Int)
case class WordFrequency(averageFrequency:Frequency, novelFrequency:Frequency, internetFrequency:Frequency, subtitlesFrequency:Frequency)
/*
case class FrequentWordRaw(novelRelative: String,
                        averageRelative: String,
                        internetOcurrences: String,
                        subtitleOcurrences: String,
                        novelOcurrences: String,
                        internetRelative: String,
                        subtitlesRelative: String,
                        word: String)
*/
case class KanjiFrequency(twitterFrequency:Frequency, aoFrequency:Frequency, newsFrequency:Frequency)

case class Kanji(literal:String,
                 jlpt:Int,
                 grade:Int,
                 meanings:List[Meaning],
                 dicNumbers:String,
                 strokeCount:Int,
                 kunYomi:List[KunYomi],
                 onYomi:List[OnYomi],
                 words:List[Word],
                 frequency:KanjiFrequency,
                 wordsFrequency:WordFrequency)
//radical, radical position, composition
