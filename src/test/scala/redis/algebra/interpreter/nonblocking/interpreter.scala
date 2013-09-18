package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient

import java.util.UUID

import akka.util.Timeout
import akka.actor.ActorSystem

import scala.concurrent.duration.Duration

import scalaz.Free
import scalaz.syntax.comonad._

import future._

trait InterpreterSpec {
  def run[A](a: Free[R, A]): A = NonBlocking.run(a, client).copoint

  def generate = s"redis-algebra-interpreter:${UUID.randomUUID.toString}"

  implicit val duration = Duration(2, "seconds")

  implicit val system = ActorSystem("redis-algebra-interpreter")

  implicit val executionContext = system.dispatcher

  implicit val timeout = Timeout(duration)

  val client = RedisClient("localhost", 6379)

  val memory = Map[String, String]()
}
