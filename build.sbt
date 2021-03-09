name := "pubsub"

version := "0.1"

scalaVersion := "2.12.13"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

val AkkaVersion = "2.6.12"


lazy val akkaVersion = "2.6.12"
lazy val akkaHttpVersion = "10.2.3"
lazy val akkaGrpcVersion = "1.1.0"
lazy val VertxVersion = "3.9.1"


libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.3" % Test,

  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % AkkaVersion,

  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,

  "com.typesafe.akka" %% "akka-http2-support" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-discovery" % akkaVersion,
  "com.typesafe.akka" %% "akka-pki" % akkaVersion,

  "services.scalable" %% "index" % "0.9",

  "io.vertx" %% "vertx-kafka-client-scala" % VertxVersion,
  "io.vertx" %% "vertx-mqtt-scala" % VertxVersion,
)

enablePlugins(AkkaGrpcPlugin)