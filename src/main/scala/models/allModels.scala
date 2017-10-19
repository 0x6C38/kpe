package models

case class Column(name: String, theType: String)
case class KanjiDic(name: String,
  columns: List[Column],
  withoutRowId: String,
  ddl: String,
  thetype: String,
  database: String,
  rows: List[List[String]])

case class FrequentWordRawParse(averageRelative: String,
                                internetOcurrences: String,
                                internetRelative: String,
                                novelOcurrences: String,
                                novelRelative: String,
                                subtitlesOcurrences: String,
                                subtitlesRelative: String,
                                word: String,
                                internetRank:String,
                                novelsRank:String,
                                subtitlesRank:String,
                                rank:String)

case class FrequentWordRawParse2(averageRelative: String,
                                 internetOcurrences: String,
                                 internetRelative: String,
                                 novelOcurrences: String,
                                 novelRelative: String,
                                 subtitlesOcurrences: String,
                                 subtitlesRelative: String,
                                 word: String,
                                 internetRank:String,
                                 novelsRank:String,
                                 subtitlesRank:String,
                                 rank:String,
                                 kanjis:List[String])

case class TanosKanji(Kunyomi:String, jlpt:Int, Onyomi:String, Kanji:String, English:String)

case class Tatoeba(secondId:String,
                   englishMeaning:String,
                   jpFurigana:String,
                   jpSentence:String,
                   firstId:String)

case class KanjiAlive(onyomi_ja:String,
                      kanji:String,
                      kmeaning:String,
                      rad_order:String,
                      kstroke:String,
                      rad_position:String,
                      kunyomi:String,
                      rad_position_ja:String,
                      rad_meaning:String,
                      onyomi:String,
                      kunyomi_ja:String,
                      rad_name:String,
                      examples:String,
                      kname:String,
                      kgrade:String,
                      radical:String,
                      rad_name_ja:String,
                      rad_stroke:String)
object Utils{
  def getInsideParens(s: String): String = {
    val parenth_contents = """.*\(([^)]+)\).*""".r
    val parenth_contents(r) = s
    r
  }
  def getStringInbetween(s:String, c1:Char, c2:Char) = {
    //val matchStr = """(\""" +"\\:".toString+ """.*)\""" + "\\(".toString
    val matchStr = """(\""" +c1.toString+ """.*)\""" + c2.toString
    val parenth_contents = matchStr.r
    val matched = parenth_contents.findFirstMatchIn(s)
    matched.map(_.group(1)).getOrElse("").tail
  }
}
case class Composition(kanji: String, role:String, phonetics: String, meaning: String)
object Composition{
  def parseKCompLine(s: String):Composition = {
    val kanji = s.head
    val inParens = Utils.getInsideParens(s).split(',')
    val role = getRole(s)
    val phonetics = inParens.head
    val meaning = inParens.last
    Composition(kanji.toString, role, phonetics, meaning)
  }

  private def getRole(s:String):String= Utils.getStringInbetween(s, ':', '(')
}

