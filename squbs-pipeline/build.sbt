import de.johoop.findbugs4sbt.FindBugs._

name := "squbs-pipeline"

val akkaV = "2.3.5"

val sprayV = "1.3.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka"         %% "akka-actor"                   % akkaV,
  "com.typesafe.akka"         %% "akka-agent"                   % akkaV,
  "com.typesafe.akka"         %% "akka-slf4j"                   % akkaV,
  "com.typesafe.akka"         %% "akka-testkit"                 % akkaV % "test",
  "io.spray"                  %% "spray-client"                 % sprayV,
  "io.spray"                  %% "spray-http"                   % sprayV,
  "org.scalatest"             %% "scalatest"                    % "2.2.1" % "test->*"
)

findbugsSettings

org.scalastyle.sbt.ScalastylePlugin.Settings

parallelExecution in Test := true

(testOptions in Test) += Tests.Argument(TestFrameworks.ScalaTest, "-h", "report/pipeline")

instrumentSettings

parallelExecution in ScoverageTest := true
