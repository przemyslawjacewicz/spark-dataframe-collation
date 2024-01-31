package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}
import pl.epsilondeltalimit.sparkdataframecollation.implicits._

case class Norm(normCase: Norm.Case /* = NormConfig.CaseConfig.None*/,
                normTrim: Norm.Trim /* = NormConfig.TrimConfig.None*/,
                normAccent: Norm.Accent /* = NormConfig.AccentConfig.None*/ ) {

  def apply(s: String): String =
    normAccent(normTrim(normCase(s)))

  def apply(c: Column, df: DataFrame): Column = {
    def applyIfString(c: Column) =
      df.select(c).schema.head.dataType match {
        case StringType => normAccent(normTrim(normCase(c))).expr
        case _          => c.expr
      }

    def go(expr: Expression): Expression =
      if (expr.children.isEmpty) applyIfString(c)
      else expr.mapChildren(e => if (e.children.isEmpty) applyIfString(new Column(e)) else go(e))

    new Column(go(c.expr)).as(c.toString())
  }
}

object Norm {

  sealed trait Case extends Product with Serializable {
    def apply(s: String): String =
      this match {
        case Case.Upper => Option(s).map(_.toUpperCase).orNull
        case Case.Lower => Option(s).map(_.toLowerCase).orNull
        case Case.None  => s
      }

    def apply(c: Column): Column =
      this match {
        case Case.Upper => upper(c)
        case Case.Lower => lower(c)
        case Case.None  => c
      }

  }

  object Case {
    case object Upper extends Case
    case object Lower extends Case
    case object None  extends Case
  }

  sealed trait Trim extends Product with Serializable {
    def apply(s: String): String =
      this match {
        case Trim.LTrim => Option(s).map(_.replaceAll("^\\s", "")).orNull
        case Trim.Trim  => Option(s).map(_.replaceAll("^\\s+|\\s+$", "")).orNull
        case Trim.RTrim => Option(s).map(_.replaceAll("\\s+$", "")).orNull
        case Trim.None  => s
      }

    def apply(c: Column): Column =
      this match {
        case Trim.LTrim => ltrim(c)
        case Trim.Trim  => trim(c)
        case Trim.RTrim => rtrim(c)
        case Trim.None  => c
      }
  }

  object Trim {
    case object LTrim extends Trim
    case object Trim  extends Trim
    case object RTrim extends Trim
    case object None  extends Trim
  }

  sealed trait Accent extends Product with Serializable {
    def apply(s: String): String =
      this match {
        case Accent.Strip => Option(s).map(_.stripAccents).orNull
        case Accent.None  => s
      }

    def apply(c: Column): Column =
      this match {
        case Accent.Strip => udf((colValue: String) => Option(colValue).map(_.stripAccents)).apply(c)
        case Accent.None  => c
      }
  }

  object Accent {
    case object Strip extends Accent
    case object None  extends Accent
  }
}
