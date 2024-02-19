package pl.epsilondeltalimit.sparkdataframecollation.collation

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Column, Dataset, Row}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.TableFor4
import org.scalatest.prop.Tables.Table
import org.scalatest.propspec.AnyPropSpec
import pl.epsilondeltalimit.sparkdataframecollation.SparkSessionProvider
import pl.epsilondeltalimit.sparkdataframecollation.normalization.Norm

class CollationDataset__Join__JoinExprs__Spec extends AnyPropSpec with SparkSessionProvider with Matchers {
  import pl.epsilondeltalimit.sparkdataframecollation.collation.implicits._
  import spark.implicits._

  // todo: find a way to abstract over norm config - stub/mock ?
  val norm: Norm = Norm(stringNorm = Norm.StringNorm(caseNorm = Norm.StringNorm.Case.Upper))

  val df: Dataset[Row]    = Seq((1, "a"), (2, "b")).toDF().as("df")
  val right: Dataset[Row] = Seq((2, "B"), (3, "C")).toDF().as("right")

  val tests: TableFor4[Column, Norm, String, Array[Row]] =
    Table(
      ("join exprs", "norm", "expected schema", "expected"),

      // inner
      (col("df._1") === col("right._1"),
       Norm(),
       "_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b", 2, "B")
       )),
      (col("df._1") === col("right._1"),
       norm,
       "_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b", 2, "B")
       )),
      (col("df._2") === col("right._2"),
       Norm(),
       "_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
       )),
      (col("df._2") === col("right._2"),
       norm,
       "_1 INT NOT NULL, _2 STRING, _1 INT NOT NULL, _2 STRING",
       Array(
         Row(2, "b", 2, "B")
       ))
    )

  property("join should fail when joined using columns selected from dataframes directly") {
    implicit val n: Norm = norm
    an[AnalysisException] should be thrownBy df.c.join(right.c, df("_1") === right("_1"))
  }

  property("join should join dataframes with normalization") {
    forAll(tests) { (joinExprs, norm, schema, expected) =>
      implicit val n: Norm = norm

      val r = df.c.join(right.c, joinExprs)

      r.schema should ===(StructType.fromDDL(schema))
      r.collect() should contain theSameElementsAs expected
    }
  }

}
