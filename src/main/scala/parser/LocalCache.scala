package parser

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Success, Try}

object LocalCache {
  def of(path: String, elseDo: => DataFrame, replaceCache: Boolean)(implicit spark: SparkSession): DataFrame = {
    (Try(spark.read.parquet(path + "-cache")), replaceCache) match {
      case (Success(vocab), _) => vocab
      case (_, false) => elseDo
      case _ => {
        val newCache = elseDo
        newCache.coalesce(1).write.mode(SaveMode.Overwrite).parquet(path + "-cache")
        newCache
      }
    }
  }
}
