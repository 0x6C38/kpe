package models

case class KanjiDS(
                kanji:String,
                level:Long,
                freq:String,
                grade:String,
                strokeCount:String,
                variant:Variant,
                examples:String,
                name:String,
                radical:Radical,
                stats:KanjiStats,
                rank:Int,
                meanings:Seq[(String, String)],
                kunYomi:Seq[Transliteration],
                onYomi:Seq[Transliteration],
                readingsWFreq:Seq[(String,String)]
                )

case class Variant(variantType:String, variant:String)
case class Radical(meaning:String, name:String, nameJP:String, order:String, position:String, positionJP:String,
                   stroke:String, radical:String)
case class Component(kanji:String, role:String, phonetics:String, meaning:String)
case class Stat(name:String, ocurrences: Int, frequency:String, rank:String)
case class KanjiStats(aoStat:Stat, twStat:Stat, wkStat:Stat, newsStat:Stat)

case class Transliteration(original:String, hiragana:String, katakana:String, romaji:String)