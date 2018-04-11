name := """clickhouse-kafka-connect"""

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "connect-api" % "0.10.0.0" % "provided",
  "com.google.code.gson" % "gson" % "2.8.0",
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.1.39",
  "org.apache.logging.log4j" % "log4j-api" % "2.8.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.8.2",

  "junit"             % "junit"           % "4.12"  % "test",
  "com.novocode"      % "junit-interface" % "0.11"  % "test"
)

// autopack plagin
packAutoSettings

// uber jar plugin

// skip tests during assembly
test in assembly := {}

//merge strategy
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}