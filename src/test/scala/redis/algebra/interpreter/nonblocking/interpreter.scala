package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient

import java.util.UUID

import akka.actor.ActorSystem
import akka.io.Tcp
import akka.util.Timeout

import org.specs2._, specification._

import scala.concurrent.duration.Duration

import scalaz.{CharSet, Free, NonEmptyList}, NonEmptyList.nel
import scalaz.syntax.Ops
import scalaz.syntax.{comonad, monad}, comonad._, monad._

import all._, future._

trait InterpreterSpec extends InterpreterSpecification

trait InterpreterSpecification extends Specification {
  override def map(fs: => Fragments) = Step(client) ^ fs ^ Step(clean) ^ Step(close)

  def run[A](a: Free[R, A]): A = NonBlocking.run(a, client).copoint

  def generate = s"${prefix}:${UUID.randomUUID.toString}".utf8

  def genkey(a: ByteString) = s"${prefix}:${UUID.randomUUID.toString}:".utf8 ++ a

  def clean = run(all.keys[R](s"${prefix}*".utf8).map(_.toList) >>= { case a :: as => all.del[R](nel(a, as)) case _ => 0L.point[F] })

  def close() = client.clientRef ! Tcp.Close

  val prefix = s"redis-algebra-interpreter:${UUID.randomUUID.toString}"

  implicit val duration = Duration(2, "seconds")

  implicit val system = ActorSystem(prefix.replace(":", "-"))

  implicit val executionContext = system.dispatcher

  implicit val timeout = Timeout(duration)

  lazy val client = RedisClient("localhost", 6379)

  implicit def StringToStringOps(a: String): StringOps = new StringOps { val self = a }

  implicit def ByteArrayToByteArrayOps(a: Array[Byte]): ByteArrayOps = new ByteArrayOps { val self = a }

  implicit def ByteStringToByteStringOps(a: ByteString): ByteStringOps = new ByteStringOps { val self = a }
}

sealed abstract class StringOps extends Ops[String] {
  final def utf8 = self.getBytes(CharSet.UTF8).toIndexedSeq
}

sealed abstract class ByteArrayOps extends Ops[Array[Byte]] {
  final def bytestring = self.toIndexedSeq
}

sealed abstract class ByteStringOps extends Ops[ByteString] {
  final def utf8 = new String(self.toArray, CharSet.UTF8)
}
