package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

package object normalization {
  object implicits {
    implicit class NormStringInterpolator(val sc: StringContext) extends AnyVal {
      def norm(args: Any*)(implicit norm: Norm): String =
        norm.stringNorm(sc.s(args: _*))
    }

    // todo: move to Column based implementation
    implicit class DatasetOps[T](ds: Dataset[T]) {
      def norm(implicit norm: Norm): Dataset[T] = {
        val r = ds.select(ds.columns.map(c => norm(col(c), ds).as(c)): _*).as[T](ds.encoder)
        new commons.implicits.DatasetOps(ds).getAlias.map(r.as).getOrElse(r) // todo: ???
      }
    }

  }
}
