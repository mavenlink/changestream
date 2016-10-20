import sbt._

object Dependencies {
  // Versions
  lazy val akkaVersion = "2.4.10"
  lazy val awsVersion = "1.11.39"

  val libraryDependencies = Seq(
    // application
    "ch.qos.logback" % "logback-classic" % "1.1.7",
    "com.typesafe" % "config" % "1.3.0",
    // akka actor system
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-agent" % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    // mysql
    "com.github.shyiko" % "mysql-binlog-connector-java" % "0.5.1",
    "com.github.mauricio" %% "mysql-async" % "0.2.20",
    // json formatter
    "io.spray" %%  "spray-json" % "1.3.2",
    // event emitter
    "com.amazonaws" % "aws-java-sdk-sqs" % awsVersion,
    "com.amazonaws" % "aws-java-sdk-sns" % awsVersion,
    "com.github.dwhjames" %% "aws-wrap" % "0.9.1"
  )

  val testDependencies = Seq(
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "it,test,bench",
    "org.scalatest" %% "scalatest" % "3.0.0" % "it,test,bench",
    "org.scalacheck" %% "scalacheck" % "1.13.2" % "it,test,bench",
    "com.storm-enroute" %% "scalameter" % "0.7" % "it,test,bench"
  )
}