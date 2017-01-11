import sbt._

object Dependencies {
  // Versions
  lazy val akkaVersion = "2.4.14"
  lazy val awsVersion = "1.11.61"
  lazy val sprayVersion = "1.3.3"

  val libraryDependencies = Seq(
    // application
    "com.typesafe" % "config" % "1.3.1",
    "org.slf4j" % "slf4j-api" % "1.7.22",
    "ch.qos.logback" % "logback-classic" % "1.1.7",
    // akka actor system
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-agent" % akkaVersion,
    "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    // spray for http control server
    "io.spray" %% "spray-can" % sprayVersion,
    "io.spray" %% "spray-routing" % sprayVersion,
    // mysql
    "com.github.shyiko" % "mysql-binlog-connector-java" % "0.6.0",
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
    "com.typesafe.akka" %% "akka-persistence-tck" % akkaVersion % "it,test,bench",
    "io.spray" %% "spray-testkit" % sprayVersion % "it,test,bench",
    "org.scalatest" %% "scalatest" % "3.0.1" % "it,test,bench",
    "com.storm-enroute" %% "scalameter" % "0.7" % "it,test,bench"
  )
}
