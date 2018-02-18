lazy val app = Project(id = "flights", base = file("."))
  .enablePlugins(DockerPlugin)
  .settings(
    scalaVersion := "2.11.11",
    resolvers ++= Seq(
      "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven",
      "Maven" at "https://repo1.maven.org/maven2/"
    ),
    libraryDependencies ++= {
      val sparkV = "2.2.1"
      val kafkaV = "0.8.2.1"
      val springV = "4.3.12.RELEASE"
      Seq(
        "org.apache.spark" %% "spark-core" % sparkV % "provided",
        "org.apache.spark" %% "spark-streaming" % sparkV % "provided",
        "org.apache.kafka" %% "kafka" % kafkaV,
        "org.apache.spark" % "spark-streaming-kafka-0-8-assembly_2.10" % sparkV,
        "org.springframework" % "spring-core" % springV,
        "org.springframework" % "spring-context" % springV,
        "org.projectlombok" % "lombok" % "1.16.20"
      )
    },
    mainClass in assembly := Some("Main"),
    assemblyJarName in assembly := "kafkaStreaming.jar",
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("com.google.**" -> "shaden_google.@1").inLibrary("com.google.inject" % "guice" % "4.1.0"),
      ShadeRule.rename("com.esotericsoftware.**" -> "shaded_esotericsoftware.@1").inAll,
      ShadeRule.rename("com.twitter.**" -> "com.ttwitterr.@1").inAll
    ),
    assemblyMergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "log4j.properties" => MergeStrategy.discard
      case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
      case "reference.conf" => MergeStrategy.concat
      case m if m endsWith ".conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )