package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.TableFor5
import org.scalatest.prop.Tables.Table
import org.scalatest.propspec.AnyPropSpec
import pl.epsilondeltalimit.sparkdataframecollation.normalization.Norm

class CollationDataset__Join__UsingColumns_JoinType__Spec extends AnyPropSpec with SparkSessionProvider with Matchers {
  import pl.epsilondeltalimit.sparkdataframecollation.collation.implicits._
  import spark.implicits._

  // todo: find a way to abstract over norm config - stub/mock ?
  val norm: Norm = Norm(stringNorm = Norm.StringNorm(caseNorm = Norm.StringNorm.Case.Upper))

  val df: Dataset[Row]    = Seq((1, "a"), (2, "b")).toDF()
  val right: Dataset[Row] = Seq((2, "B"), (3, "C")).toDF()

  val tests: TableFor5[Seq[String], String, Norm, String, Array[Row]] =
    Table(
      ("using columns", "join type", "norm", "expected schema", "expected"),

      // inner
      (Seq("_1"),
       "inner",
       Norm(),
       "_1 INT NOT NULL, _2 STRING, _2 STRING",
       Array(
         Row(2, "b", "B")
       )),
      (Seq("_1"),
       "inner",
       norm,
       "_1 INT NOT NULL, _2 STRING, _2 STRING",
       Array(
         Row(2, "b", "B")
       )),
      (Seq("_2"),
       "inner",
       Norm(),
       "_2 STRING, _1 INT NOT NULL, _1 INT NOT NULL",
       Array(
       )),
      (Seq("_2"),
       "inner",
       norm,
       "_2 STRING, _1 INT NOT NULL, _1 INT NOT NULL",
       Array(
         Row("B", 2, 2)
       )),

      // cross
      (Seq("_1"),
       "cross",
       Norm(),
       "_1 INT NOT NULL, _2 STRING, _2 STRING",
       Array(
         Row(2, "b", "B")
       )),
      (Seq("_1"),
       "cross",
       norm,
       "_1 INT NOT NULL, _2 STRING, _2 STRING",
       Array(
         Row(2, "b", "B")
       )),
      (Seq("_2"),
       "cross",
       Norm(),
       "_2 STRING, _1 INT NOT NULL, _1 INT NOT NULL",
       Array(
       )),
      (Seq("_2"),
       "cross",
       norm,
       "_2 STRING, _1 INT NOT NULL, _1 INT NOT NULL",
       Array(
         Row("B", 2, 2)
       )),

      // full outer
      (Seq("_1"),
       "full_outer",
       Norm(),
       "_1 INT, _2 STRING, _2 STRING",
       Array(
         Row(3, null, "C"),
         Row(1, "a", null),
         Row(2, "b", "B")
       )),
      (Seq("_1"),
       "full_outer",
       norm,
       "_1 INT, _2 STRING, _2 STRING",
       Array(
         Row(3, null, "C"),
         Row(1, "a", null),
         Row(2, "b", "B")
       )),
      (Seq("_2"),
       "full_outer",
       Norm(),
       "_2 STRING, _1 INT, _1 INT",
       Array(
         Row("B", null, 2),
         Row("C", null, 3),
         Row("a", 1, null),
         Row("b", 2, null)
       )),
      (Seq("_2"),
       "full_outer",
       norm,
       "_2 STRING, _1 INT, _1 INT",
       Array(
         Row("C", null, 3),
         Row("A", 1, null),
         Row("B", 2, 2)
       )),

      // left outer
      (Seq("_1"),
       "left_outer",
       Norm(),
       "_1 INT NOT NULL, _2 STRING, _2 STRING",
       Array(
         Row(1, "a", null),
         Row(2, "b", "B")
       )),
      (Seq("_1"),
       "left_outer",
       norm,
       "_1 INT NOT NULL, _2 STRING, _2 STRING",
       Array(
         Row(1, "a", null),
         Row(2, "b", "B")
       )),
      (Seq("_2"),
       "left_outer",
       Norm(),
       "_2 STRING, _1 INT NOT NULL, _1 INT",
       Array(
         Row("a", 1, null),
         Row("b", 2, null)
       )),
      (Seq("_2"),
       "left_outer",
       norm,
       "_2 STRING, _1 INT NOT NULL, _1 INT",
       Array(
         Row("A", 1, null),
         Row("B", 2, 2)
       )),

      // right outer
      (Seq("_1"),
       "right_outer",
       Norm(),
       "_1 INT NOT NULL, _2 STRING, _2 STRING",
       Array(
         Row(3, null, "C"),
         Row(2, "b", "B")
       )),
      (Seq("_1"),
       "right_outer",
       norm,
       "_1 INT NOT NULL, _2 STRING, _2 STRING",
       Array(
         Row(3, null, "C"),
         Row(2, "b", "B")
       )),
      (Seq("_2"),
       "right_outer",
       Norm(),
       "_2 STRING, _1 INT, _1 INT NOT NULL",
       Array(
         Row("B", null, 2),
         Row("C", null, 3)
       )),
      (Seq("_2"),
       "right_outer",
       norm,
       "_2 STRING, _1 INT, _1 INT NOT NULL",
       Array(
         Row("C", null, 3),
         Row("B", 2, 2)
       )),

      // left semi
      (Seq("_1"),
       "left_semi",
       Norm(),
       "_1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b")
       )),
      (Seq("_1"),
       "left_semi",
       norm,
       "_1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b")
       )),
      (Seq("_2"),
       "left_semi",
       Norm(),
       "_2 STRING, _1 INT NOT NULL",
       Array(
       )),
      (Seq("_2"),
       "left_semi",
       norm,
       "_2 STRING, _1 INT NOT NULL",
       Array(
         Row("b", 2)
       )),

      // left anti
      (Seq("_1"),
       "left_anti",
       Norm(),
       "_1 INT NOT NULL, _2 STRING",
       Array(
         Row(1, "a")
       )),
      (Seq("_1"),
       "left_anti",
       norm,
       "_1 INT NOT NULL, _2 STRING",
       Array(
         Row(1, "a")
       )),
      (Seq("_2"),
       "left_anti",
       Norm(),
       "_2 STRING, _1 INT NOT NULL",
       Array(
         Row("a", 1),
         Row("b", 2)
       )),
      (Seq("_2"),
       "left_anti",
       norm,
       "_2 STRING, _1 INT NOT NULL",
       Array(
         Row("a", 1)
       ))
    )

  property("join should join dataframes with normalization") {
    forAll(tests) { (usingColumns, joinType, norm, schema, expected) =>
      implicit val n: Norm = norm

      val r = df.c.join(right.c, usingColumns, joinType)

      r.schema should ===(StructType.fromDDL(schema))
      r.collect() should contain theSameElementsAs expected
    }
  }

}
