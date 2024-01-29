package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import pl.epsilondeltalimit.sparkdataframecollation

case class CollationDataFrame(df: DataFrame) {

  val alias = getAlias(df)

  private def getAlias(df: DataFrame) = df.queryExecution.analyzed match {
    case SubqueryAlias(identifier, _) => Some(identifier.name)
    case _ => None
  }

  def distinct(): CollationDataFrame =
    new CollationDataFrame(df.select(upper(col("value"))).distinct())

  def join(right: DataFrame, joinExprs: Column, joinType: String): CollationDataFrame = {
    val normJoinExprs = new Column(joinExprs.expr.mapChildren(c => upper(new Column(c)).expr))

    new CollationDataFrame(df.join(right, normJoinExprs, joinType))

    //      new Wrapper(
    //        df.select(upper(col("value")).as("value"))
    //          .as(alias.get)
    //          .join(right.select(upper(col("value")).as("value")).as(getAlias(right).get), joinExprs, joinType)
    //      )
  }

  def where(condition: Column): CollationDataFrame = {
    val normCondition = new Column(condition.expr.mapChildren(c => upper(new Column(c)).expr))

    new CollationDataFrame(df.where(normCondition))
  }

}
