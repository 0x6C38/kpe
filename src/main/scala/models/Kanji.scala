package models

import com.atilika.kuromoji.ipadic.Tokenizer
import sjt.JapaneseInstances._
import sjt.JapaneseSyntax._
//import bayio.kpe.{Frequency, Word}

case class KanaTransliteration(original:String, hiragana:String, katakana:String, romaji:String) extends Serializable
object KanaTransliteration{
  val tokenizerCache = new Tokenizer()
  def apply(s:String) = {
    new KanaTransliteration(s, s.toHiragana(tokenizerCache), s.toKatakana(tokenizerCache), s.toRomaji(tokenizerCache))
  }
}
