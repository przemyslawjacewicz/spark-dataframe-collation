package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias

import java.text.Normalizer

package object commons {
  object implicits {
    implicit class StringWithStripAccents(val str: String) extends AnyVal {
      def stripAccents: String = Normalizer
        .normalize(str, Normalizer.Form.NFD)
        .replaceAll("\\p{InCombiningDiacriticalMarks}+", "")
    }

    implicit class DatasetOps(ds: Dataset[_]) {
      def getAlias: Option[String] = ds.queryExecution.analyzed match {
        case SubqueryAlias(identifier, _) => Some(identifier.name)
        case _                            => None
      }
    }

  }
}
