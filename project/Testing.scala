import sbt.Keys._
import sbt._

object Testing {
  lazy val Benchmark = config("bench") extend Test
  lazy val benchmark = TaskKey[Unit]("bench")
  lazy val testScalastyle = TaskKey[Unit]("testScalastyle")
  lazy val testAll = TaskKey[Unit]("test-all")

  def itFilter(name: String): Boolean = name endsWith "ISpec"
  def unitFilter(name: String): Boolean = (name endsWith "Spec") && !itFilter(name)
  def benchFilter(name: String): Boolean = name endsWith "Bench"

  private lazy val testSettings =
    inConfig(Test)(Defaults.testSettings) ++
      Seq(
// TODO: get scalastyle working again
//        testScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Test).toTask("").value,
//        (test in Test) <<= (test in Test) dependsOn testScalastyle,
        resourceDirectory in testScalastyle := baseDirectory.value / "src/test/resources",
        testOptions in Test := Seq(Tests.Filter(unitFilter))
      )

  private lazy val itSettings =
    inConfig(IntegrationTest)(Defaults.testSettings) ++
      Seq(
        fork in IntegrationTest := false,
        logBuffered in IntegrationTest := false,
        parallelExecution in IntegrationTest := false,
        testOptions in IntegrationTest := Seq(Tests.Filter(itFilter)),
        resourceDirectory in IntegrationTest := baseDirectory.value / "src/test/resources",
        scalaSource in IntegrationTest := baseDirectory.value / "src/test/scala")

  private lazy val benchSettings =
    inConfig(Benchmark)(Defaults.testSettings) ++
      Seq(
        testFrameworks in Benchmark += new TestFramework("org.scalameter.ScalaMeterFramework"),
        fork in Benchmark := false,
        logBuffered in Benchmark := false,
        parallelExecution in Benchmark := false,
        testOptions in Benchmark := Seq(Tests.Filter(benchFilter)),
        resourceDirectory in Benchmark := baseDirectory.value / "src/test/resources",
        scalaSource in Benchmark := baseDirectory.value / "src/test/scala")

  lazy val settings = testSettings ++ itSettings ++ benchSettings ++ Seq(
    libraryDependencies ++= Dependencies.testDependencies,
    testAll := (test in Benchmark).dependsOn((test in IntegrationTest).dependsOn(test in Test)).value
  )

  lazy val configs = Seq(
    IntegrationTest,
    Benchmark
  )
}
