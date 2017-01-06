val nextRelease = "0.2.0"
val scalaVer = "2.11.8"

lazy val projectInfo = Seq(
  name := "changestream",
  organization := "mavenlink",
  maintainer := "Mavenlink <oss@mavenlink.com>",
  packageSummary := "Change Stream",
  packageDescription := "Streams changes to SQS/Redis from a MySQL Binlog stream"
)

lazy val changestream = (project in file(".")).
  enablePlugins(GitVersioning, GitBranchPrompt).
  enablePlugins(JavaAppPackaging).
  enablePlugins(sbtdocker.DockerPlugin).
  settings(projectInfo: _*).
  settings(Testing.settings: _*).
  configs(Testing.configs: _*).
  settings(
    scalaVersion := scalaVer,
    scalacOptions := Seq("-deprecation", "-unchecked", "-feature"),
    // sbt-git
    git.baseVersion := nextRelease,
    git.formattedShaVersion := git.gitHeadCommit.value map {sha =>
      s"${nextRelease}-" + sys.props.getOrElse("version", default = s"$sha")
    },
    // package settings
    exportJars := true,
    debianPackageDependencies in Debian ++= Seq("oraclejdk"),
    mappings in Universal ++= Seq(
      ((resourceDirectory in Compile).value / "application.conf") -> "conf/application.conf",
      ((resourceDirectory in Compile).value / "logback.xml") -> "conf/logback.xml"
    ),
    // docker settings
    dockerfile in docker := {
      new Dockerfile {
        from("openjdk:8-jre")
        entryPoint(s"/app/bin/${executableScriptName.value}")
        copy(stage.value, "/app")
      }
    },
    libraryDependencies ++= Dependencies.libraryDependencies,
    resolvers ++= Resolvers.list
  )
