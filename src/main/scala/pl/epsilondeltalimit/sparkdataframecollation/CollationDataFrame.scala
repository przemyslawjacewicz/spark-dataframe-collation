package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

case class CollationDataFrame(df: DataFrame)(implicit spark: SparkSession, norm: Norm) {

  val alias = getAlias(df)

  private def getAlias(df: DataFrame) = df.queryExecution.analyzed match {
    case SubqueryAlias(identifier, _) => Some(identifier.name)
    case _                            => None
  }

  def join(right: DataFrame, usingColumn: String): CollationDataFrame = {
    val dfNorm     = normWithStaging(df)
    val rightNorm  = normWithStaging(right)
    val joinedNorm = dfNorm.join(rightNorm, usingColumn)
    val dfNormColumns = dfNorm
      .drop(usingColumn, stagingColumnName(usingColumn))
      .columns
      .filter(isStagingColumn)
      .map(c => dfNorm(c).as(sourceColumnName(c)))
    val rightNormColumns = rightNorm
      .drop(usingColumn, stagingColumnName(usingColumn))
      .columns
      .filter(isStagingColumn)
      .map(c => rightNorm(c).as(sourceColumnName(c)))
    val r = joinedNorm.select(col(usingColumn) +: (dfNormColumns ++ rightNormColumns): _*)

    CollationDataFrame(r)
  }

  def join(right: DataFrame, usingColumns: Seq[String]): CollationDataFrame = {
    val dfNorm     = normWithStaging(df)
    val rightNorm  = normWithStaging(right)
    val joinedNorm = dfNorm.join(rightNorm, usingColumns)
    val dfNormColumns = dfNorm
      .drop(usingColumns.flatMap(usingColumn => Seq(usingColumn, stagingColumnName(usingColumn))): _*)
      .columns
      .filter(isStagingColumn)
      .map(c => dfNorm(c).as(sourceColumnName(c)))
    val rightNormColumns = rightNorm
      .drop(usingColumns.flatMap(usingColumn => Seq(usingColumn, stagingColumnName(usingColumn))): _*)
      .columns
      .filter(isStagingColumn)
      .map(c => rightNorm(c).as(sourceColumnName(c)))
    val r = joinedNorm.select(usingColumns.map(col) ++ dfNormColumns ++ rightNormColumns: _*)

    CollationDataFrame(r)
  }

  def join(right: DataFrame, usingColumns: Seq[String], joinType: String): CollationDataFrame = {
    val dfNorm     = normWithStaging(df)
    val rightNorm  = normWithStaging(right)
    val joinedNorm = dfNorm.join(rightNorm, usingColumns, joinType)
    val dfNormColumns = dfNorm
      .drop(usingColumns.flatMap(usingColumn => Seq(usingColumn, stagingColumnName(usingColumn))): _*)
      .columns
      .filter(isStagingColumn)
      .map(c => dfNorm(c).as(sourceColumnName(c)))
    val rightNormColumns = rightNorm
      .drop(usingColumns.flatMap(usingColumn => Seq(usingColumn, stagingColumnName(usingColumn))): _*)
      .columns
      .filter(isStagingColumn)
      .map(c => rightNorm(c).as(sourceColumnName(c)))
    val r = joinedNorm.select(usingColumns.map(col) ++ dfNormColumns ++ rightNormColumns: _*)

    CollationDataFrame(r)
  }

  def join(right: DataFrame, joinExprs: Column): CollationDataFrame = {
    val dfNorm     = normWithStaging(df)
    val rightNorm  = normWithStaging(right)
    val joinedNorm = dfNorm.join(rightNorm, joinExprs)
    val dfNormColumns = dfNorm.columns
      .filter(isStagingColumn)
      .map(c => dfNorm(c).as(sourceColumnName(c)))
    val rightNormColumns = rightNorm.columns
      .filter(isStagingColumn)
      .map(c => rightNorm(c).as(sourceColumnName(c)))
    val r = joinedNorm.select(dfNormColumns ++ rightNormColumns: _*)

    CollationDataFrame(r)
  }

  def join(right: DataFrame, joinExprs: Column, joinType: String): CollationDataFrame = {
    val dfNorm     = normWithStaging(df)
    val rightNorm  = normWithStaging(right)
    val joinedNorm = dfNorm.join(rightNorm, joinExprs, joinType)
    val dfNormColumns = dfNorm.columns
      .filter(isStagingColumn)
      .map(c => dfNorm(c).as(sourceColumnName(c)))
    val rightNormColumns = rightNorm.columns
      .filter(isStagingColumn)
      .map(c => rightNorm(c).as(sourceColumnName(c)))
    val r = joinedNorm.select(dfNormColumns ++ rightNormColumns: _*)

    CollationDataFrame(r)
  }

//  def joinWith[U](other: Dataset[U], condition: Column, joinType: String): Dataset[Dataset(T, U)] = {
//    ???
//  }

//  def joinWith[U](other: Dataset[U], condition: Column): Dataset[(T, U)] = {
//    ???
//  }

  def sort(sortCol: String, sortCols: String*): DataFrame =
    ???

  def sort(sortExprs: Column*): DataFrame =
    ???

  def orderBy(sortCol: String, sortCols: String*): DataFrame =
    ???

  def orderBy(sortExprs: Column*): DataFrame =
    ???

  def filter(condition: Column): CollationDataFrame =
    ???
  def filter(conditionExpr: String): CollationDataFrame =
    ???

  def where(condition: Column): CollationDataFrame =
    CollationDataFrame(df.where(norm(condition, df)))
//  def where(conditionExpr: String): CollationDataFrame =
//    CollationDataFrame(df.where(norm(condition, df)))

  def groupBy(cols: Column*) =
    ???

  def rollup(cols: Column*) =
    ???

  def cube(cols: Column*) =
    ???

  def groupBy(col1: String, cols: String*) =
    ???

//  def groupByKey[K: Encoder](func: T => K): KeyValueGroupedDataset[K, T] =
//    ???

//  def groupByKey[K](func: MapFunction[T, K], encoder: Encoder[K]): KeyValueGroupedDataset[K, T] =
//    ???

  def rollup(col1: String, cols: String*) =
    ???

  def cube(col1: String, cols: String*) =
    ???

  def intersect(other: DataFrame) =
    ???

  def intersectAll(other: DataFrame) =
    ???

  def except(other: DataFrame): DataFrame =
    ???

  def exceptAll(other: DataFrame) =
    ???

// repartition ???

  // todo: add randomness -> return either a or A, not depending on norm
  def distinct(): CollationDataFrame = {
    val dfNorm = norm(df)
    val r      = dfNorm.distinct()

    CollationDataFrame(r)
  }

  private def norm(df: DataFrame): DataFrame =
    df.select(df.columns.map(c => norm(col(c), df)): _*)

  private def normWithStaging(df: DataFrame): DataFrame =
    df.select(df.columns.flatMap(c => Seq(col(c).as(stagingColumnName(c)), norm(col(c), df))): _*)

  private def stagingColumnName(c: String): String =
    s"${c}__old"

  private def isStagingColumn(c: String): Boolean =
    c.endsWith("__old")

  private def sourceColumnName(c: String): String =
    c.dropRight(5)

}
