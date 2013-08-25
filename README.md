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

import akka.util.Timeout
import akka.actor.ActorSystem

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import scalaz.NonEmptyList, NonEmptyList._
import scalaz.std.list._
import scalaz.syntax.all._

import redis.algebra.{F, R}
import redis.algebra.{KeyAlgebra, ListAlgebra, StringAlgebra}, KeyAlgebra._, ListAlgebra._, StringAlgebra._
import redis.algebra.interpreter.nonblocking.NonBlocking

val e0 =
  set[R]("key", "value") >>
  get[R]("key")

val e1 =
  set[R]("counter", "100") >>
  incr[R]("counter") >>
  incr[R]("counter") >>
  incrby[R]("counter", 10)

val e2 =
  List("first", "second", "third").map(a => rpush[R]("messages", nels(a))).sequenceU >>
  lrange[R]("messages", 0, 2)

val duration = Duration(2, "seconds")

implicit val system = ActorSystem("redis-algebra-interpreter")

implicit val executionContext = system.dispatcher

implicit val timeout = Timeout(duration)

val client = RedisClient("localhost", 6379)

val r0 = Await.result(NonBlocking.run(e0, client), duration)

val r1 = Await.result(NonBlocking.run(e1, client), duration)

val r2 = Await.result(NonBlocking.run(e2, client), duration)

println(r0)
// Some(value)

println(r1)
// 112

println(r2)
// List(first, second, third)
```
