
name := "BTCPriceStream"

scalaVersion := "2.12.20"

version := "1.0"

libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0",
    "org.apache.spark" %% "spark-core" % "3.5.5",
    "org.apache.spark" %% "spark-sql" % "3.5.5",
    "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.5"
)