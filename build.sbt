import Common.*
import Common.Library.implicits
import sbt.project

name    := "spark-dataframe-collation"
version := "0.1"

updateSbtClassifiers / useCoursier := true

lazy val root = (project in file("."))
  .settings(Common.settings(ProjectVersion(0, 1)))
  .settings(
    libraryDependencies ++= Library.spark,
    libraryDependencies ++= Library.scopt,
    libraryDependencies ++= Library.logging,
    libraryDependencies ++= Library.pureConfig,
    libraryDependencies ++= Library.sparkTests % Test,
    libraryDependencies ++= Library.scalaTest  % Test,
    libraryDependencies ++= Library.scalaMock  % Test
  )
