scalaVersion in ThisBuild := "2.11.7"

name := "pubsub"

organization in ThisBuild := "org.squbs.samples"

version in ThisBuild := "0.8.0-SNAPSHOT"

publishArtifact := false

checksums in ThisBuild := Nil

fork in ThisBuild := true

lazy val pubsubsvc = project

resolvers in ThisBuild ++= Seq(
  "eBay Central Releases" at "http://ebaycentral/content/repositories/releases/",
  "eBay Central Snapshots" at "http://ebaycentral/content/repositories/snapshots/",
  "Maven Central" at "http://ebaycentral/content/repositories/central/"
)