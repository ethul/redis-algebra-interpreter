package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient
import com.redis.protocol.{ANil, ArgsOps, RedisCommand}, RedisCommand.Args
import com.redis.serialization.DefaultWriters.{anyWriter => _, _}

import akka.util.{ByteString => AkkaByteString, Timeout}

import com.redis.protocol.StringCommands.{EX, PX, NX, XX}

import scala.concurrent.{ExecutionContext, Future}

import scalaz.std.option._
import scalaz.syntax.all._
import scalaz.syntax.std.{boolean, option}, boolean._, option._

import data.{And, Error, Not, Nx, Ok, Or, Xor, Xx}, future._, syntax._

trait NonBlockingStringInstance extends StringInstances {
  implicit def stringAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[StringAlgebra] =
    new NonBlocking[StringAlgebra] {
      def runAlgebra[A](algebra: StringAlgebra[A], client: RedisClient) =
        algebra match {
          case Append(k, v, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: v.toArray +: ANil)).map(h(_))
          case Bitcount(k, s, e, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: s.fzip(e).cata(a => a._1 +: a._2 +: ANil, ANil))).map(h(_))
          case Bitop(op @ And(d, k), h) =>
            client.ask(Command[Long](algebra.command, op.command +: d.toArray +: k.map(_.toArray).list.toArgs)).map(h(_))
          case Bitop(op @ Or(d, k), h) =>
            client.ask(Command[Long](algebra.command, op.command +: d.toArray +: k.map(_.toArray).list.toArgs)).map(h(_))
          case Bitop(op @ Xor(d, k), h) =>
            client.ask(Command[Long](algebra.command, op.command +: d.toArray +: k.map(_.toArray).list.toArgs)).map(h(_))
          case Bitop(op @ Not(d, k), h) =>
            client.ask(Command[Long](algebra.command, op.command +: d.toArray +: k.toArray +: ANil)).map(h(_))
          case Decr(k, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: ANil)).map(h(_))
          case Decrby(k, d, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: d +: ANil)).map(h(_))
          case Get(k, h) =>
            client.ask(Command[Option[AkkaByteString]](algebra.command, k.toArray +: ANil)).map(h(_))
          case Getbit(k, o, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: o +: ANil)).map(h(_))
          case Getrange(k, s, t, h) =>
            client.ask(Command[Option[AkkaByteString]](algebra.command, k.toArray +: s +: t +: ANil)).map(a => h(a.cata(a => a, AkkaByteString.empty)))
          case Getset(k, v, h) =>
            client.ask(Command[Option[AkkaByteString]](algebra.command, k.toArray +: v.toArray +: ANil)).map(h(_))
          case Incr(k, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: ANil)).map(h(_))
          case Incrby(k, i, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: i +: ANil)).map(h(_))
          case Incrbyfloat(k, i, h) =>
            client.ask(Command[String](algebra.command, k.toArray +: i.toString +: ANil)).map(a => h(BigDecimal(a)))
          case Mget(k, h) =>
            client.ask(Command[Seq[Option[AkkaByteString]]](algebra.command, k.list.map(_.toArray).toArgs)).map(h(_))
          case Mset(p, h) =>
            client.ask(Command[Boolean](algebra.command, p.foldRight(ANil){case ((k, v), a) => k.toArray +: v.toArray +: a})).map(a => h(a.fold(Ok, Error)))
          case Msetnx(p, h) =>
            client.ask(Command[Boolean](algebra.command, p.foldRight(ANil){case ((k, v), a) => k.toArray +: v.toArray +: a})).map(h(_))
          case Psetex(k, i, v, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: i +: v.toArray +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Set(k, v, i, o, h) =>
            client.ask(Command[Boolean](algebra.command,
              k.toArray +:
              v.toArray +:
              i.cata(_.fold(EX +: _ +: ANil, PX +: _ +: ANil), ANil).values ++:
              o.cata(_.toString.toUpperCase +: ANil, ANil)
            )).map(h(_))
          case Setbit(k, o, v, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: o +: v.fold(1.toString, 0.toString) +: ANil)).map(h(_))
          case Setex(k, i, v, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: i +: v.toArray +: ANil)).map(a => h(a.fold(Ok, Error)))
          case Setnx(k, v, h) =>
            client.ask(Command[Boolean](algebra.command, k.toArray +: v.toArray +: ANil)).map(h(_))
          case Setrange(k, o, v, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: o +: v.toArray +: ANil)).map(h(_))
          case Strlen(k, h) =>
            client.ask(Command[Long](algebra.command, k.toArray +: ANil)).map(h(_))
        }

      val EX = "EX"
      val PX = "PX"
    }
}
