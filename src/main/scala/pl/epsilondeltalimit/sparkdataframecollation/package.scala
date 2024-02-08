package pl.epsilondeltalimit

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.Normalizer

package object sparkdataframecollation {

  object implicits {

    implicit class NormStringInterpolator(val sc: StringContext) extends AnyVal {
      def norm(args: Any*)(implicit norm: Norm): String =
        norm(sc.s(args: _*))
    }

    implicit class StringWithStripAccents(val str: String) extends AnyVal {
      def stripAccents: String = Normalizer
        .normalize(str, Normalizer.Form.NFD)
        .replaceAll("\\p{InCombiningDiacriticalMarks}+", "")
    }

    implicit class DataFrameWithCollation(df: DataFrame) {
      def c(implicit spark: SparkSession, norm: Norm): CollationDataFrame =
        CollationDataFrame(df)
    }

    implicit def unwrap(cdf: CollationDataFrame): DataFrame =
      cdf.df

  }
}
