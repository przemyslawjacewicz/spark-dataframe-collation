package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

case class CollationDataFrame(df: DataFrame)(implicit norm: Norm) {

  val alias = getAlias(df)

  private def getAlias(df: DataFrame) = df.queryExecution.analyzed match {
    case SubqueryAlias(identifier, _) => Some(identifier.name)
    case _                            => None
  }

  // todo: add randomness -> return either a or A, not depending on norm
  def distinct(): CollationDataFrame =
//    CollationDataFrame(df.select(df.columns.map(col).map(c => norm(c, df)): _*).distinct())
    ???

  def join(right: DataFrame, joinExprs: Column, joinType: String): CollationDataFrame = {
    println("=== DEBUG: start ===")
    val dfNorm = df.select(df.columns.flatMap(c => Seq(col(c).as(s"${c}__old"), norm(col(c), df))): _*)
    dfNorm.show()
    val rightNorm = right.select(right.columns.flatMap(c => Seq(col(c).as(s"${c}__old"), norm(col(c), right))): _*)
    rightNorm.show()
    val joined = dfNorm.join(rightNorm, joinExprs, joinType)
    joined.show()

//    val dropped = joined.select( joined.columns.filterNot( _.endsWith("__old")   ).map(col) : _*    )
    val dropped =
      joined.drop(dfNorm.columns.filterNot(_.endsWith("__old")): _*).drop(right.columns.filterNot(_.endsWith("__old")): _*)
    dropped.show()

    val spark = SparkSession.getActiveSession.get
//    val xx = dropped.schema.map(f => f.copy(name = f.name.dropRight(5))     )
    val r = spark.createDataFrame(dropped.rdd, StructType( dropped.schema.map(f => f.copy(name = f.name.dropRight(5))     ) ))
r.show()
    println("=== DEBUG: end ===")

    CollationDataFrame(r)

//    CollationDataFrame(df.join(right, norm(joinExprs, df), joinType))
  }

  def where(condition: Column): CollationDataFrame =
    CollationDataFrame(df.where(norm(condition, df)))

}
