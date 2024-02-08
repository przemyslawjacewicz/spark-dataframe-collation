package pl.epsilondeltalimit.sparkdataframecollation

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}
import pl.epsilondeltalimit.sparkdataframecollation.implicits._

//todo: make this more generic
case class Norm(normCase: Norm.Case = Norm.Case.None,
                normTrim: Norm.Trim = Norm.Trim.None,
                normAccent: Norm.Accent = Norm.Accent.None) {

  // todo: consider a different method because it looks like a variant of norm for column but based on string
  def apply(s: String): String =
    normAccent(normTrim(normCase(s)))

  def apply(c: Column, df: DataFrame): Column = {
    // note: applicable for leaf expression (no children)
    def applyForLeaf(expr: Expression): Expression = {
      println(s"apply: expr=$expr")
      val r = df.selectExpr(expr.sql).schema.head.dataType match {
        case StringType =>
          df.queryExecution.analyzed.output.find(_.sql.endsWith(expr.sql)) match {
            case Some(a) => normAccent(normTrim(normCase(col(a.sql)))).expr
            case None    => normAccent(normTrim(normCase(lit(expr.sql)))).expr
          }
        case _ => expr
      }

      println(s"apply: r=$r")
      r
    }

    def go(expr: Expression): Expression =
      if (expr.children.isEmpty) applyForLeaf(expr)
      else expr.mapChildren(e => if (e.children.isEmpty) applyForLeaf(e) else go(e))

    new Column(go(c.expr))
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
