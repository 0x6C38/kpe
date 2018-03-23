package parser

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import parser.Hello.spark

object EdictParser {
  def parseEdict(path: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val parseEdict = udf { (r: String) =>
      val parts = r.split('/').map(_.trim)
      val word = parts.head.split('[').head.trim
      word
    }

    val parseEdictTs = udf { (r: String) =>
      val parts = r.split('/').map(_.trim)
      val translations = parts.tail.filterNot(s => s == null || s.isEmpty).map { s =>
        val shitInParens: Array[String] = Option(StringUtils.substringsBetween(s, "(", ")")).getOrElse(Array[String]()).filterNot((j: String) => j == null || j.isEmpty())
        val numsInParens = shitInParens.map(_.trim).filter(StringUtils.isNumeric)

        def removeUnwanted(seqs: Seq[String], from: String) = seqs.foldLeft(from)((z, i) => StringUtils.remove(z, i))

        (removeUnwanted(shitInParens.map(i => "(" + i + ")"), s).trim)
      }.filterNot(s => s == null || s.isEmpty).toSeq
      translations
    }

    val edict = spark.read.text(path)
      .withColumn("edictWord", parseEdict('value))
      .withColumn("translations", parseEdictTs('value))
      .dropDuplicates("edictWord") //should better join the duplicate data together but we
      .drop('value)
    edict
  }
}
