package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient

import java.util.UUID

import akka.util.Timeout
import akka.actor.ActorSystem

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import scalaz.Free

trait InterpreterSpec {
  def run[A](a: Free[R, A]): A = Await.result(NonBlocking.run(a, client), duration)

  def generate = s"redis-algebra-interpreter:${UUID.randomUUID.toString}"

  val duration = Duration(2, "seconds")

  implicit val system = ActorSystem("redis-algebra-interpreter")

  implicit val executionContext = system.dispatcher

  implicit val timeout = Timeout(duration)

  val client = RedisClient("localhost", 6379)
}
