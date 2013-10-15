# Redis Algebra Interpreter

An interpreter for running programs written using the [Redis Algebra](https://github.com/ethul/redis-algebra). The interpreter uses the non-blocking Redis client [Scala Redis NB](https://github.com/debasishg/scala-redis-nb) under the hood for performing the commands of the algebra.

# Install

Currently, a snapshot of the Redis Algebra Interpreter is available in the following repository. The snippet below may be added to an SBT build file in order to use the Redis Algebra Interpreter.

```scala
libraryDependencies += "redis-algebra-interpreter" %% "redis-algebra-interpreter" % "0.0.1-SNAPSHOT"

resolvers += "Github ethul/ivy-repository snapshots" at "https://github.com/ethul/ivy-repository/raw/master/snapshots/"
```

# Usage

```scala
import com.redis.RedisClient

import akka.actor.ActorSystem
import akka.util.Timeout

import scala.concurrent.duration.Duration

import scalaz.{CharSet, NonEmptyList}, NonEmptyList.nels
import scalaz.std.list._
import scalaz.syntax.{Ops, comonad, monad, traverse}, comonad._, monad._, traverse._
import scalaz.syntax.std.list._

import redis.algebra.{R, all}, all._
import redis.algebra.interpreter.nonblocking.{NonBlocking, future}, future._

val e0 =
  set[R]("key".utf8, "value".utf8) >>
  get[R]("key".utf8)

val e1 =
  set[R]("counter".utf8, "100".utf8) >>
  incr[R]("counter".utf8) >>
  incr[R]("counter".utf8) >>
  incrby[R]("counter".utf8, 10)

val e2 =
  List("first".utf8, "second".utf8, "third".utf8).map(a => rpush[R]("messages".utf8, nels(a))).sequenceU >>
  lrange[R]("messages".utf8, 0, 2)

implicit val duration = Duration(2, "seconds")

implicit val system = ActorSystem("redis-algebra-interpreter")

implicit val executionContext = system.dispatcher

implicit val timeout = Timeout(duration)

lazy val client = RedisClient("localhost", 6379)

implicit def StringToStringOps(a: String): StringOps = new StringOps { val self = a }

sealed abstract class StringOps extends Ops[String] { final def utf8 = self.getBytes(CharSet.UTF8).toIndexedSeq }

val r0 = NonBlocking.run(e0, client).copoint

val r1 = NonBlocking.run(e1, client).copoint

val r2 = NonBlocking.run(e2, client).copoint

println(r0)
// Some(ByteString(118, 97, 108, 117, 101))

println(r1)
// 112

println(r2)
// Vector(ByteString(102, 105, 114, 115, 116), ByteString(115, 101, 99, 111, 110, 100), ByteString(116, 104, 105, 114, 100))
```
