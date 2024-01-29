package pl.epsilondeltalimit

import org.apache.spark.sql.DataFrame

package object sparkdataframecollation {

  implicit class DataFrameWithCollation(df: DataFrame) {

    def c: CollationDataFrame =
      new CollationDataFrame(df)

  }

}
