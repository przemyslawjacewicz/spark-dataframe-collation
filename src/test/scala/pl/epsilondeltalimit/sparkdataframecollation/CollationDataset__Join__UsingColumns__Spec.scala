package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.{TableFor4, TableFor5}
import org.scalatest.prop.Tables.Table
import org.scalatest.propspec.AnyPropSpec
import pl.epsilondeltalimit.sparkdataframecollation.normalization.Norm

class CollationDataset__Join__UsingColumns__Spec extends AnyPropSpec with SparkSessionProvider with Matchers {
  import pl.epsilondeltalimit.sparkdataframecollation.collation.implicits._
  import spark.implicits._

  // todo: find a way to abstract over norm config - stub/mock ?
  val norm: Norm = Norm(stringNorm = Norm.StringNorm(caseNorm = Norm.StringNorm.Case.Upper))

  val df: Dataset[Row] = Seq((1, "a"), (2, "b")).toDF()
  val right: Dataset[Row] = Seq((2, "B"), (3, "C")).toDF()

  val tests: TableFor4[Seq[String], Norm, String, Array[Row]] =
    Table(
      ("using columns", "norm", "expected schema", "expected"),

      (Seq("_1"),
        Norm(),
        "_1 INT NOT NULL, _2 STRING, _2 STRING",
        Array(
          Row(2, "b", "B")
        )),
      (Seq("_1"),
        norm,
        "_1 INT NOT NULL, _2 STRING, _2 STRING",
        Array(
          Row(2, "b", "B")
        )),
      (Seq("_2"),
        Norm(),
        "_2 STRING, _1 INT NOT NULL, _1 INT NOT NULL",
        Array(
        )),
      (Seq("_2"),
        norm,
        "_2 STRING, _1 INT NOT NULL, _1 INT NOT NULL",
        Array(
          Row("B", 2, 2)
        )),

    )

  property("join should join dataframes with normalization") {
    forAll(tests) { (usingColumns, norm, schema, expected) =>
      implicit val n: Norm = norm

      val r = df.c.join(right.c, usingColumns)

      r.schema should ===(StructType.fromDDL(schema))
      r.collect() should contain theSameElementsAs expected
    }
  }

}