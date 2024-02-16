package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.{Dataset, Row}
import pl.epsilondeltalimit.sparkdataframecollation.normalization.Norm

package object collation {

  type CollationDataFrame = CollationDataset[Row]

  object implicits {
    implicit class DatasetOps[T](ds: Dataset[T]) {
      // todo: consider a better name
      def c(implicit norm: Norm): CollationDataset[T] =
        new CollationDataset[T](ds)
    }

  }
}
