package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient
import com.redis.protocol.{ANil, ArgsOps, RedisCommand}, RedisCommand.Args
import com.redis.serialization.DefaultWriters.{anyWriter => _, _}

import akka.util.{ByteString => AkkaByteString, Timeout}

import scala.collection.immutable.{Set => ScalaSet}
import scala.concurrent.{ExecutionContext, Future}

import scalaz.syntax.std.option._

import future._, syntax._

trait NonBlockingSetInstance extends SetInstances {
  implicit def setAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[SetAlgebra] =
    new NonBlocking[SetAlgebra] {
      def runAlgebra[A](algebra: SetAlgebra[A], client: RedisClient) =
        algebra match {
          case Sadd(k, m, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: m.map(_.toArray).list.toArgs)).map(h(_))
          case Scard(k, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: ANil)).map(h(_))
          case Sdiff(k, h) =>
            client.ask(Command[ScalaSet[AkkaByteString]](algebra.command, k.map(_.toArray).list.toArgs)).map(a => h(a.map(_.toIndexedSeq)))
          case Sdiffstore(d, k, h) =>
            client.ask(Command[Long](algebra.command, d.toArray +: k.map(_.toArray).list.toArgs)).map(h(_))
          case Sinter(k, h) =>
            client.ask(Command[ScalaSet[AkkaByteString]](algebra.command, k.map(_.toArray).list.toArgs)).map(a => h(a.map(_.toIndexedSeq)))
          case Sinterstore(d, k, h) =>
            client.ask(Command[Long](algebra.command, d.toArray +: k.map(_.toArray).list.toArgs)).map(h(_))
          case Sismember(k, m, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: m.toArray +: ANil)).map(h(_))
          case Smembers(k, h) =>
            client.ask(Command[ScalaSet[AkkaByteString]](algebra.command, k.toArray +: ANil)).map(a => h(a.map(_.toIndexedSeq)))
          case Smove(s, d, m, h) =>
            client.ask(Command[Boolean](algebra.command, s.toArray +: d.toArray +: m.toArray +: ANil)).map(h(_))
          case Spop(k, h) =>
            client.ask(Command[Option[AkkaByteString]](algebra.command, k.toArray +: ANil)).map(h(_))
          case Srandmember(k, c, h) =>
            client.ask(Command[ScalaSet[AkkaByteString]](algebra.command, k.toArray +: c.cata(_ +: ANil, ANil))).map(a => h(a.map(_.toIndexedSeq)))
          case Srem(k, m, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: m.map(_.toArray).list.toArgs)).map(h(_))
          case Sunion(k, h) =>
            client.ask(Command[ScalaSet[AkkaByteString]](algebra.command, k.map(_.toArray).list.toArgs)).map(a => h(a.map(_.toIndexedSeq)))
          case Sunionstore(d, k, h) =>
            client.ask(Command[Long](algebra.command, d.toArray +: k.map(_.toArray).list.toArgs)).map(h(_))
        }
    }
}
