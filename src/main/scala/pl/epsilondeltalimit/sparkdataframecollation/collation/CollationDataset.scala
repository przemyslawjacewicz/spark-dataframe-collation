package pl.epsilondeltalimit.sparkdataframecollation.collation

import org.apache.spark.api.java.function._
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, KeyValueGroupedDataset, RelationalGroupedDataset, Row, SparkSession, TypedColumn}
import org.apache.spark.storage.StorageLevel
import pl.epsilondeltalimit.sparkdataframecollation.collation.CollationDataset._
import pl.epsilondeltalimit.sparkdataframecollation.normalization
import pl.epsilondeltalimit.sparkdataframecollation.normalization.Norm

import java.util
import scala.reflect.runtime.universe

//todo: add docs to overridden methods to use aliasing for Column expressions
//todo: super -> super.toDF()
class CollationDataset[T](sparkSession: SparkSession, logicalPlan: LogicalPlan, encoder: Encoder[T])(
    implicit norm: Norm)
    extends Dataset[T](sparkSession, logicalPlan, encoder) {

  def this(ds: Dataset[T])(implicit norm: Norm) =
    this(ds.sparkSession, ds.queryExecution.logical, ds.encoder)

//  override val encoder: Encoder[T]             = super.encoder
//  override val queryExecution: QueryExecution  = super.queryExecution
//  override lazy val sparkSession: SparkSession = super.sparkSession
//  override lazy val sqlContext: SQLContext     = super.sqlContext

  override def toDF(): CollationDataFrame =
    new CollationDataFrame(super.toDF())

  override def as[U: Encoder]: CollationDataset[U] =
    new CollationDataset[U](super.as)

  // todo: should this return CollationDataFrame ?
  override def toDF(colNames: String*): DataFrame =
    new CollationDataFrame(super.toDF(colNames: _*))

  override def checkpoint(): CollationDataset[T] =
    new CollationDataset[T](super.checkpoint())

  override def checkpoint(eager: Boolean): CollationDataset[T] =
    new CollationDataset[T](super.checkpoint(eager))

  override def localCheckpoint(): CollationDataset[T] =
    new CollationDataset[T](super.localCheckpoint())

  override def localCheckpoint(eager: Boolean): CollationDataset[T] =
    new CollationDataset[T](super.localCheckpoint(eager))

  override def withWatermark(eventTime: String, delayThreshold: String): CollationDataset[T] =
    new CollationDataset[T](super.withWatermark(eventTime, delayThreshold))

  // todo: check me
//  override def na: DataFrameNaFunctions = super.na

  // todo: check me
//  override def stat: DataFrameStatFunctions = super.stat

  // todo: implement me
  override def join(right: Dataset[_]): CollationDataFrame =
    new CollationDataFrame(super.join(right))

  override def join(right: Dataset[_], usingColumn: String): CollationDataFrame =
    join(right, Seq(usingColumn))

  override def join(right: Dataset[_], usingColumns: Seq[String]): CollationDataFrame =
    join(right, usingColumns, "inner")

  override def join(right: Dataset[_], usingColumns: Seq[String], joinType: String): CollationDataFrame = {
    val dfNormWithStagingColumns    = normWithStagingColumns(super.toDF())
    val rightNormWithStagingColumns = normWithStagingColumns(right.toDF())

    val joinedNormWithStagingColumns =
      dfNormWithStagingColumns.join(rightNormWithStagingColumns, usingColumns, joinType)

    val dropColumnNames = usingColumns.flatMap(usingColumn => Seq(usingColumn, stagingColumnName(usingColumn)))

    val columns = joinType match {
      case "semi" | "leftsemi" | "left_semi" | "anti" | "leftanti" | "left_anti" =>
        usingColumns.map(c => joinedNormWithStagingColumns(stagingColumnName(c)).as(c)) ++ stripStagingColumns(
          dfNormWithStagingColumns.drop(dropColumnNames: _*))
      case _ =>
        usingColumns.map(joinedNormWithStagingColumns.apply) ++ stripStagingColumns(
          dfNormWithStagingColumns.drop(dropColumnNames: _*)) ++ stripStagingColumns(
          rightNormWithStagingColumns.drop(dropColumnNames: _*))
    }

    val r = joinedNormWithStagingColumns.select(columns: _*)

    new CollationDataFrame(r)
  }

  override def join(right: Dataset[_], joinExprs: Column): CollationDataFrame =
    join(right, joinExprs, "inner")

  override def join(right: Dataset[_], joinExprs: Column, joinType: String): CollationDataFrame = {
    val dfNormWithStagingColumns    = normWithStagingColumns(super.toDF())
    val rightNormWithStagingColumns = normWithStagingColumns(right.toDF())

    // todo: norm joinExprs ???
    val joinedNormWithStagingColumns = dfNormWithStagingColumns.join(rightNormWithStagingColumns, joinExprs, joinType)

    val columns = joinType match {
      case "semi" | "leftsemi" | "left_semi" | "anti" | "leftanti" | "left_anti" =>
        stripStagingColumns(dfNormWithStagingColumns)
      case _ =>
        stripStagingColumns(dfNormWithStagingColumns) ++ stripStagingColumns(rightNormWithStagingColumns)
    }

    val r = joinedNormWithStagingColumns.select(columns: _*)

    new CollationDataFrame(r)
  }

  override def crossJoin(right: Dataset[_]): CollationDataFrame =
    new CollationDataFrame(super.crossJoin(right))

  // todo: implement me
  override def joinWith[U](other: Dataset[U], condition: Column, joinType: String): CollationDataset[(T, U)] =
    new CollationDataset[(T, U)](super.joinWith(other, condition, joinType))

  // todo: implement me
  override def joinWith[U](other: Dataset[U], condition: Column): CollationDataset[(T, U)] =
    new CollationDataset[(T, U)](super.joinWith(other, condition))

  // todo: implement me
  override def sortWithinPartitions(sortCol: String, sortCols: String*): CollationDataset[T] =
    new CollationDataset[T](super.sortWithinPartitions(sortCol, sortCols: _*))

  // todo: implement me
  override def sortWithinPartitions(sortExprs: Column*): CollationDataset[T] =
    new CollationDataset[T](super.sortWithinPartitions(sortExprs: _*))

  // todo: implement me
  override def sort(sortCol: String, sortCols: String*): CollationDataset[T] =
    new CollationDataset[T](super.sort(sortCol, sortCols: _*))

  // todo: implement me
  override def sort(sortExprs: Column*): CollationDataset[T] =
    new CollationDataset[T](super.sort(sortExprs: _*))

  // todo: implement me
  override def orderBy(sortCol: String, sortCols: String*): CollationDataset[T] =
    new CollationDataset[T](super.orderBy(sortCol, sortCols: _*))

  // todo: implement me
  override def orderBy(sortExprs: Column*): CollationDataset[T] =
    new CollationDataset[T](super.orderBy(sortExprs: _*))

  override def hint(name: String, parameters: Any*): CollationDataset[T] =
    new CollationDataset[T](super.hint(name, parameters))

  override def as(alias: String): CollationDataset[T] =
    new CollationDataset[T](super.as(alias))

  override def as(alias: Symbol): CollationDataset[T] =
    new CollationDataset[T](super.as(alias))

  override def alias(alias: String): CollationDataset[T] =
    new CollationDataset[T](super.alias(alias))

  override def alias(alias: Symbol): CollationDataset[T] =
    new CollationDataset[T](super.alias(alias))

  override def select(cols: Column*): CollationDataFrame =
    new CollationDataFrame(super.select(cols: _*))

  override def select(col: String, cols: String*): CollationDataFrame =
    new CollationDataFrame(super.select(col, cols: _*))

  override def selectExpr(exprs: String*): CollationDataFrame =
    new CollationDataFrame(super.selectExpr(exprs: _*))

  override def select[U1](c1: TypedColumn[T, U1]): CollationDataset[U1] =
    new CollationDataset[U1](super.select(c1))

  // todo: ???
//  override protected def selectUntyped(columns: TypedColumn[_, _]*): CollationDataset[_] =
//    new CollationDataset[_](super.selectUntyped(columns: _*))

  override def select[U1, U2](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2]): CollationDataset[(U1, U2)] =
    new CollationDataset[(U1, U2)](super.select(c1, c2))

  override def select[U1, U2, U3](c1: TypedColumn[T, U1],
                                  c2: TypedColumn[T, U2],
                                  c3: TypedColumn[T, U3]): CollationDataset[(U1, U2, U3)] =
    new CollationDataset[(U1, U2, U3)](super.select(c1, c2, c3))

  override def select[U1, U2, U3, U4](c1: TypedColumn[T, U1],
                                      c2: TypedColumn[T, U2],
                                      c3: TypedColumn[T, U3],
                                      c4: TypedColumn[T, U4]): CollationDataset[(U1, U2, U3, U4)] =
    new CollationDataset[(U1, U2, U3, U4)](super.select(c1, c2, c3, c4))

  override def select[U1, U2, U3, U4, U5](c1: TypedColumn[T, U1],
                                          c2: TypedColumn[T, U2],
                                          c3: TypedColumn[T, U3],
                                          c4: TypedColumn[T, U4],
                                          c5: TypedColumn[T, U5]): CollationDataset[(U1, U2, U3, U4, U5)] =
    new CollationDataset[(U1, U2, U3, U4, U5)](super.select(c1, c2, c3, c4, c5))

  override def filter(condition: Column): CollationDataset[T] = {
    val dfNormWithStagingColumns = normWithStagingColumns(super.toDF())

    val filteredNormWithStagingColumns = dfNormWithStagingColumns.filter(norm(condition, this).toString())

    val columns = stripStagingColumns(dfNormWithStagingColumns)
    val r       = filteredNormWithStagingColumns.select(columns: _*).as[T](encoder)

    new CollationDataset[T](r)
  }

  // todo: implement me
  override def filter(conditionExpr: String): CollationDataset[T] =
    filter(new Column(sparkSession.sessionState.sqlParser.parseExpression(conditionExpr)))

  override def where(condition: Column): CollationDataset[T] =
    filter(condition)

  // todo: implement me
  override def where(conditionExpr: String): CollationDataset[T] =
    new CollationDataset[T](super.where(conditionExpr))

  // todo: implement me
  override def groupBy(cols: Column*): RelationalGroupedDataset =
    super.groupBy(cols: _*)

  // todo: implement me
  override def rollup(cols: Column*): RelationalGroupedDataset =
    super.rollup(cols: _*)

  // todo: implement me
  override def cube(cols: Column*): RelationalGroupedDataset =
    super.cube(cols: _*)

  // todo: implement me
  override def groupBy(col1: String, cols: String*): RelationalGroupedDataset =
    super.groupBy(col1, cols: _*)

  // todo: implement me
  override def groupByKey[K: Encoder](func: T => K): KeyValueGroupedDataset[K, T] =
    super.groupByKey(func)

  // todo: implement me
  override def groupByKey[K](func: MapFunction[T, K], encoder: Encoder[K]): KeyValueGroupedDataset[K, T] =
    super.groupByKey(func, encoder)

  // todo: implement me
  override def rollup(col1: String, cols: String*): RelationalGroupedDataset =
    super.rollup(col1, cols: _*)

  // todo: implement me
  override def cube(col1: String, cols: String*): RelationalGroupedDataset =
    super.cube(col1, cols: _*)

  // todo: implement me
  override def agg(aggExpr: (String, String), aggExprs: (String, String)*): CollationDataFrame =
    new CollationDataFrame(super.agg(aggExpr, aggExprs: _*))

  // todo: implement me
  override def agg(exprs: Map[String, String]): CollationDataFrame =
    new CollationDataFrame(super.agg(exprs))

  // todo: implement me
  override def agg(exprs: util.Map[String, String]): CollationDataFrame =
    new CollationDataFrame(super.agg(exprs))

  // todo: implement me
  override def agg(expr: Column, exprs: Column*): CollationDataFrame =
    new CollationDataFrame(super.agg(expr, exprs: _*))

  override def observe(name: String, expr: Column, exprs: Column*): CollationDataset[T] =
    new CollationDataset[T](super.observe(name, expr, exprs: _*))

  override def limit(n: Int): CollationDataset[T] =
    new CollationDataset[T](super.limit(n))

  override def union(other: Dataset[T]): CollationDataset[T] =
    new CollationDataset[T](super.union(other))

  override def unionAll(other: Dataset[T]): CollationDataset[T] =
    new CollationDataset[T](super.unionAll(other))

  override def unionByName(other: Dataset[T]): CollationDataset[T] =
    new CollationDataset[T](super.unionByName(other))

  override def unionByName(other: Dataset[T], allowMissingColumns: Boolean): CollationDataset[T] =
    new CollationDataset[T](super.unionByName(other, allowMissingColumns))

  // todo: implement me
  override def intersect(other: Dataset[T]): CollationDataset[T] =
    new CollationDataset[T](super.intersect(other))

  // todo: implement me
  override def intersectAll(other: Dataset[T]): CollationDataset[T] =
    new CollationDataset[T](super.intersectAll(other))

  // todo: implement me
  override def except(other: Dataset[T]): CollationDataset[T] =
    new CollationDataset[T](super.except(other))

  // todo: implement me
  override def exceptAll(other: Dataset[T]): CollationDataset[T] =
    new CollationDataset[T](super.exceptAll(other))

  override def sample(fraction: Double, seed: Long): CollationDataset[T] =
    new CollationDataset[T](super.sample(fraction, seed))

  override def sample(fraction: Double): CollationDataset[T] =
    new CollationDataset[T](super.sample(fraction))

  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): CollationDataset[T] =
    new CollationDataset[T](super.sample(withReplacement, fraction, seed))

  override def sample(withReplacement: Boolean, fraction: Double): CollationDataset[T] =
    new CollationDataset[T](super.sample(withReplacement, fraction))

  // todo: implement me
  override def randomSplit(weights: Array[Double], seed: Long): Array[Dataset[T]] =
    super.randomSplit(weights, seed)

  // todo: implement me
  override def randomSplitAsList(weights: Array[Double], seed: Long): util.List[Dataset[T]] =
    super.randomSplitAsList(weights, seed)

  // todo: implement me
  override def randomSplit(weights: Array[Double]): Array[Dataset[T]] =
    super.randomSplit(weights)

  override def explode[A <: Product: universe.TypeTag](input: Column*)(
      f: Row => TraversableOnce[A]): CollationDataFrame =
    new CollationDataFrame(super.explode(input: _*)(f))

  override def explode[A, B: universe.TypeTag](inputColumn: String, outputColumn: String)(
      f: A => TraversableOnce[B]): CollationDataFrame =
    new CollationDataFrame(super.explode(inputColumn, outputColumn)(f))

  override def withColumn(colName: String, col: Column): CollationDataFrame =
    new CollationDataFrame(super.withColumn(colName, col))

  override def withColumnRenamed(existingName: String, newName: String): CollationDataFrame =
    new CollationDataFrame(super.withColumnRenamed(existingName, newName))

  override def drop(colName: String): CollationDataFrame =
    new CollationDataFrame(super.drop(colName))

  override def drop(colNames: String*): CollationDataFrame =
    new CollationDataFrame(super.drop(colNames: _*))

  override def drop(col: Column): CollationDataFrame =
    new CollationDataFrame(super.drop(col))

  // todo: implement me
  override def dropDuplicates(): CollationDataset[T] =
    new CollationDataset[T](super.dropDuplicates())

  // todo: implement me
  override def dropDuplicates(colNames: Seq[String]): CollationDataset[T] =
    new CollationDataset[T](super.dropDuplicates(colNames))

  override def dropDuplicates(colNames: Array[String]): CollationDataset[T] =
    new CollationDataset[T](super.dropDuplicates(colNames))

  override def dropDuplicates(col1: String, cols: String*): CollationDataset[T] =
    new CollationDataset[T](super.dropDuplicates(col1, cols: _*))

  override def describe(cols: String*): CollationDataFrame =
    new CollationDataFrame(super.describe(cols: _*))

  override def summary(statistics: String*): CollationDataFrame =
    new CollationDataFrame(super.summary(statistics: _*))

  override def transform[U](t: Dataset[T] => Dataset[U]): CollationDataset[U] =
    new CollationDataset[U](super.transform(t))

  override def filter(func: T => Boolean): CollationDataset[T] =
    new CollationDataset[T](super.filter(func))

  override def filter(func: FilterFunction[T]): CollationDataset[T] =
    new CollationDataset[T](super.filter(func))

  override def map[U: Encoder](func: T => U): CollationDataset[U] =
    new CollationDataset[U](super.map(func))

  override def map[U](func: MapFunction[T, U], encoder: Encoder[U]): CollationDataset[U] =
    new CollationDataset[U](super.map(func, encoder))

  override def mapPartitions[U: Encoder](func: Iterator[T] => Iterator[U]): CollationDataset[U] =
    new CollationDataset[U](super.mapPartitions(func))

  override def mapPartitions[U](f: MapPartitionsFunction[T, U], encoder: Encoder[U]): CollationDataset[U] =
    new CollationDataset[U](super.mapPartitions(f, encoder))

  override def flatMap[U: Encoder](func: T => TraversableOnce[U]): CollationDataset[U] =
    new CollationDataset[U](super.flatMap(func))

  override def flatMap[U](f: FlatMapFunction[T, U], encoder: Encoder[U]): CollationDataset[U] =
    new CollationDataset[U](super.flatMap(f, encoder))

  override def repartition(numPartitions: Int): CollationDataset[T] =
    new CollationDataset[T](super.repartition(numPartitions))

  // todo: implement me
  override def repartition(numPartitions: Int, partitionExprs: Column*): CollationDataset[T] =
    new CollationDataset[T](super.repartition(numPartitions, partitionExprs: _*))

  // todo: implement me
  override def repartition(partitionExprs: Column*): CollationDataset[T] =
    new CollationDataset[T](super.repartition(partitionExprs: _*))

  // todo: implement me
  override def repartitionByRange(numPartitions: Int, partitionExprs: Column*): CollationDataset[T] =
    new CollationDataset[T](super.repartitionByRange(numPartitions, partitionExprs: _*))

  // todo: implement me
  override def repartitionByRange(partitionExprs: Column*): CollationDataset[T] =
    new CollationDataset[T](super.repartitionByRange(partitionExprs: _*))

  override def coalesce(numPartitions: Int): CollationDataset[T] =
    new CollationDataset[T](super.coalesce(numPartitions))

  // todo: check me
  override def distinct(): CollationDataset[T] = {
    println("=== distinct ===")
    println(s"this: ${this.getClass.getSimpleName}")
    val dfNorm = new normalization.implicits.DatasetOps(super.toDF()).norm
    println(s"dfNorm: ${dfNorm.getClass.getSimpleName}")
    val r = dfNorm.distinct().as[T](encoder)
    println(s"r: ${r.getClass.getSimpleName}")

    new CollationDataset[T](r)
  }

  override def persist(): CollationDataset.this.type = {
    super.persist()
    this
  }

  override def cache(): CollationDataset.this.type = {
    super.cache()
    this
  }

  override def persist(newLevel: StorageLevel): CollationDataset.this.type = {
    super.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean): CollationDataset.this.type = {
    super.unpersist(blocking)
    this
  }

  override def unpersist(): CollationDataset.this.type = {
    super.unpersist()
    this
  }

  override def toJSON: CollationDataset[String] =
    new CollationDataset[String](super.toJSON)

}

object CollationDataset {

  private def normWithStagingColumns(df: DataFrame)(implicit norm: Norm): DataFrame =
    df
      .select(
        df.queryExecution.analyzed.output
          .flatMap(a =>
            if (a.resolved)
              Seq(
                new Column(Alias(df(a.qualifiedName).expr, stagingColumnName(a.name))(qualifier = a.qualifier)),
                new Column(Alias(norm(df(a.qualifiedName), df).expr, a.name)(qualifier = a.qualifier))
              )
            else
              Seq(
                new Column(Alias(df(a.name).expr, stagingColumnName(a.name))()),
                new Column(Alias(norm(df(a.name), df).expr, a.name)())
              )): _*
      )

  private def stripStagingColumns(df: DataFrame): Array[Column] =
    df.queryExecution.analyzed.output
      .filter(a => isStagingColumn(a.name))
      .map { a =>
        if (a.resolved)
          new Column(Alias(df(a.qualifiedName).expr, sourceColumnName(a.name))(qualifier = a.qualifier))
        else
          new Column(Alias(df(a.qualifiedName).expr, sourceColumnName(a.name))())
      }
      .toArray

  private def stagingColumnName(sourceColName: String): String =
    s"${sourceColName}__old"

  private def isStagingColumn(c: String): Boolean =
    c.endsWith("__old")

  private def sourceColumnName(stagingColName: String): String =
    stagingColName.dropRight(5)

}
