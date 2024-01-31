package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

//todo: convert upper to norm config
case class CollationDataFrame(df: DataFrame)(implicit norm: Norm) {

  val alias = getAlias(df)

  private def getAlias(df: DataFrame) = df.queryExecution.analyzed match {
    case SubqueryAlias(identifier, _) => Some(identifier.name)
    case _                            => None
  }

  // todo: add randomness -> return either a or A, not depending on norm
  def distinct(): CollationDataFrame =
    CollationDataFrame(df.select(df.columns.map(col).map(c => norm(c, df)): _*).distinct())

  def join(right: DataFrame, joinExprs: Column, joinType: String): CollationDataFrame =
    CollationDataFrame(df.join(right, norm(joinExprs, df), joinType))

  def where(condition: Column): CollationDataFrame =
    CollationDataFrame(df.where(norm(condition, df)))

}
