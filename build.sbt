name := "redis-algebra-interpreter"

organization := "com.github.ethul"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.10.2"

libraryDependencies += "com.github.ethul" %% "redis-algebra" % "0.0.1-SNAPSHOT"

libraryDependencies += "net.debasishg" %% "redisreact" % "0.2"

libraryDependencies += "org.specs2" %% "specs2" % "2.2-scalaz-7.1.0-SNAPSHOT" % "test"

resolvers += "Github ethul releases" at "https://github.com/ethul/ivy-repository/raw/master/releases/"

resolvers += "Github ethul snapshots" at "https://github.com/ethul/ivy-repository/raw/master/snapshots/"

resolvers += "Sonatype releases" at "http://oss.sonatype.org/content/repositories/releases/"

resolvers += "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"

scalacOptions += "-feature"

scalacOptions += "-deprecation"

scalacOptions += "-language:higherKinds"

scalacOptions += "-language:implicitConversions"

scalacOptions += "-Xlint"

scalacOptions += "-Xfatal-warnings"

scalacOptions += "-Yno-adapted-args"

scalacOptions += "-Ywarn-all"

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath + "/tmp/scala/ivy-repo/snapshots")))
