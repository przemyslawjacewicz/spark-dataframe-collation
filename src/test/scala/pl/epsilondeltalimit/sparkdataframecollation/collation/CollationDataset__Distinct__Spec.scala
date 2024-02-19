package pl.epsilondeltalimit.sparkdataframecollation.collation

import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.prop.TableFor3
import org.scalatest.prop.Tables.Table
import org.scalatest.propspec.AnyPropSpec
import pl.epsilondeltalimit.sparkdataframecollation.SparkSessionProvider
import pl.epsilondeltalimit.sparkdataframecollation.normalization.Norm

class CollationDataset__Distinct__Spec extends AnyPropSpec with SparkSessionProvider with Matchers {
  import pl.epsilondeltalimit.sparkdataframecollation.collation.implicits._
  import spark.implicits._

  val norm: Norm = Norm(stringNorm = Norm.StringNorm(caseNorm = Norm.StringNorm.Case.Upper))

  val fallbackTests: TableFor3[Seq[Int], Norm, Array[Row]] = Table(
    ("input", "norm", "expected"),
    (Seq(1, 1, 2), Norm(), Array(Row(1), Row(2))),
    (Seq(1, 1, 2), norm, Array(Row(1), Row(2)))
  )

  val simpleTests: TableFor3[Seq[String], Norm, Array[Row]] = Table(
    ("input", "norm", "expected"),
    (Seq("a", "A", "b"), Norm(), Array(Row("a"), Row("A"), Row("b"))),
    (Seq("a", "A", "b"), norm, Array(Row("A"), Row("B"))) // todo: should B be b ?
  )

  val tests: TableFor3[Seq[(Int, String)], Norm, Array[Row]] = Table(
    ("input", "norm", "expected"),
    (Seq((1, "a"), (1, "A"), (2, "b")), Norm(), Array(Row(1, "a"), Row(1, "A"), Row(2, "b"))),
    (Seq((1, "a"), (1, "A"), (2, "b")), norm, Array(Row(1, "A"), Row(2, "B")))
  )

  property("distinct should deduplicate with normalization") {
    forAll(tests) { case (input, norm, expected) =>
      implicit val n: Norm = norm
      val df               = input.toDF()

      val r = df.c.distinct()

      r.schema should ===(df.schema)
      r.collect() should contain theSameElementsAs expected
    }
  }

}
