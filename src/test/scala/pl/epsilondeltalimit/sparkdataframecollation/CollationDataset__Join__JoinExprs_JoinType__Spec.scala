package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Column, Dataset, Row}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import org.scalatest.prop.{TableFor1, TableFor5}
import org.scalatest.propspec.AnyPropSpec
import pl.epsilondeltalimit.sparkdataframecollation.normalization.Norm

class CollationDataset__Join__JoinExprs_JoinType__Spec extends AnyPropSpec with SparkSessionProvider with Matchers {
  import pl.epsilondeltalimit.sparkdataframecollation.collation.implicits._
  import spark.implicits._

  // todo: find a way to abstract over norm config - stub/mock ?
  val norm: Norm = Norm(stringNorm = Norm.StringNorm(caseNorm = Norm.StringNorm.Case.Upper))

  val df: Dataset[Row]    = Seq((1, "a"), (2, "b")).toDF().as("df")
  val right: Dataset[Row] = Seq((2, "B"), (3, "C")).toDF().as("right")

  val joinTypes: TableFor1[String] = Table(
    "joinType",
    "inner",
    "cross",
    "full_outer",
    "left_outer",
    "right_outer",
    "left_semi",
    "left_anti"
  )

  val tests: TableFor5[Column, String, Norm, String, Array[Row]] =
    Table(
      ("join exprs", "join type", "norm", "expected schema", "expected"),

      // inner
      (col("df._1") === col("right._1"),
       "inner",
       Norm(),
       "_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b", 2, "B")
       )),
      (col("df._1") === col("right._1"),
       "inner",
       norm,
       "_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b", 2, "B")
       )),
      (col("df._2") === col("right._2"),
       "inner",
       Norm(),
       "_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
       )),
      (col("df._2") === col("right._2"),
       "inner",
       norm,
       "_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b", 2, "B")
       )),

      // cross
      (col("df._1") === col("right._1"),
       "cross",
       Norm(),
       "_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b", 2, "B")
       )),
      (col("df._1") === col("right._1"),
       "cross",
       norm,
       "_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b", 2, "B")
       )),
      (col("df._2") === col("right._2"),
       "cross",
       Norm(),
       "_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
       )),
      (col("df._2") === col("right._2"),
       "cross",
       norm,
       "_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b", 2, "B")
       )),

      // full outer
      (col("df._1") === col("right._1"),
       "full_outer",
       Norm(),
       "_1 INT, _2 STRING, _1 INT, _2 STRING",
       Array(
         Row(null, null, 3, "C"),
         Row(1, "a", null, null),
         Row(2, "b", 2, "B")
       )),
      (col("df._1") === col("right._1"),
       "full_outer",
       norm,
       "_1 INT, _2 STRING, _1 INT, _2 STRING",
       Array(
         Row(null, null, 3, "C"),
         Row(1, "a", null, null),
         Row(2, "b", 2, "B")
       )),
      (col("df._2") === col("right._2"),
       "full_outer",
       Norm(),
       "_1 INT, _2 STRING, _1 INT, _2 STRING",
       Array(
         Row(null, null, 2, "B"),
         Row(null, null, 3, "C"),
         Row(1, "a", null, null),
         Row(2, "b", null, null)
       )),
      (col("df._2") === col("right._2"),
       "full_outer",
       norm,
       "_1 INT, _2 STRING, _1 INT, _2 STRING",
       Array(
         Row(null, null, 3, "C"),
         Row(1, "a", null, null),
         Row(2, "b", 2, "B")
       )),

      // left outer
      (col("df._1") === col("right._1"),
       "left_outer",
       Norm(),
       "_1 INT NOT NULL, _2 STRING, _1 INT, _2 STRING",
       Array(
         Row(1, "a", null, null),
         Row(2, "b", 2, "B")
       )),
      (col("df._1") === col("right._1"),
       "left_outer",
       norm,
       "_1 INT NOT NULL, _2 STRING, _1 INT, _2 STRING",
       Array(
         Row(1, "a", null, null),
         Row(2, "b", 2, "B")
       )),
      (col("df._2") === col("right._2"),
       "left_outer",
       Norm(),
       "_1 INT NOT NULL, _2 STRING, _1 INT, _2 STRING",
       Array(
         Row(1, "a", null, null),
         Row(2, "b", null, null)
       )),
      (col("df._2") === col("right._2"),
       "left_outer",
       norm,
       "_1 INT NOT NULL, _2 STRING, _1 INT, _2 STRING",
       Array(
         Row(1, "a", null, null),
         Row(2, "b", 2, "B")
       )),

      // right outer
      (col("df._1") === col("right._1"),
       "right_outer",
       Norm(),
       "_1 INT, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
         Row(null, null, 3, "C"),
         Row(2, "b", 2, "B")
       )),
      (col("df._1") === col("right._1"),
       "right_outer",
       norm,
       "_1 INT, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
         Row(null, null, 3, "C"),
         Row(2, "b", 2, "B")
       )),
      (col("df._2") === col("right._2"),
       "right_outer",
       Norm(),
       "_1 INT, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
         Row(null, null, 2, "B"),
         Row(null, null, 3, "C")
       )),
      (col("df._2") === col("right._2"),
       "right_outer",
       norm,
       "_1 INT, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
         Row(null, null, 3, "C"),
         Row(2, "b", 2, "B")
       )),

      // left semi
      (col("df._1") === col("right._1"),
       "left_semi",
       Norm(),
       "_1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b")
       )),
      (col("df._1") === col("right._1"),
       "left_semi",
       norm,
       "_1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b")
       )),
      (col("df._2") === col("right._2"),
       "left_semi",
       Norm(),
       "_1 INT NOT NULL, _2 STRING",
       Array(
       )),
      (col("df._2") === col("right._2"),
       "left_semi",
       norm,
       "_1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b")
       )),

      // left anti
      (col("df._1") === col("right._1"),
       "left_anti",
       Norm(),
       "_1 INT NOT NULL, _2 STRING",
       Array(
         Row(1, "a")
       )),
      (col("df._1") === col("right._1"),
       "left_anti",
       norm,
       "_1 INT NOT NULL, _2 STRING",
       Array(
         Row(1, "a")
       )),
      (col("df._2") === col("right._2"),
       "left_anti",
       Norm(),
       "_1 INT NOT NULL, _2 STRING",
       Array(
         Row(1, "a"),
         Row(2, "b")
       )),
      (col("df._2") === col("right._2"),
       "left_anti",
       norm,
       "_1 INT NOT NULL, _2 STRING",
       Array(
         Row(1, "a")
       ))
    )

  property("join should fail when joined using columns selected from dataframes directly") {
    forAll(joinTypes) { joinType =>
      implicit val n: Norm = Norm()
      an[AnalysisException] should be thrownBy df.c.join(right.c, df("_1") === right("_1"), joinType)
      an[AnalysisException] should be thrownBy df.c.join(right.c, df("_2") === right("_2"), joinType)
    }
  }

  property("join should join dataframes with normalization") {
    forAll(tests) { (joinExprs, joinType, norm, schema, expected) =>
      implicit val n: Norm = norm

      val r = df.c.join(right.c, joinExprs, joinType)

      r.schema should ===(StructType.fromDDL(schema))
      r.collect() should contain theSameElementsAs expected
    }
  }

}
