package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient

import akka.util.Timeout

import com.redis.protocol.StringCommands.{EX, PX, NX, XX}

import scala.concurrent.{ExecutionContext, Future}

import scalaz.{-\/, \/-}
import scalaz.std.option._
import scalaz.syntax.zip._
import scalaz.syntax.std.{boolean, option}, boolean._, option._

import future._

trait NonBlockingStringInstance extends StringInstances {
  implicit def stringAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[StringAlgebra] =
    new NonBlocking[StringAlgebra] {
      def runAlgebra[A](algebra: StringAlgebra[A], client: RedisClient) =
        algebra match {
          case Append(k, v, h) =>
            client.append(k, v).map(h(_))
          case Bitcount(k, s, e, h) =>
            client.bitcount(k, s.fzip(e)).map(h(_))
          case Bitop(And(d, k), h) =>
            client.bitop("AND", d, k.list).map(h(_))
          case Bitop(Or(d, k), h) =>
            client.bitop("OR", d, k.list).map(h(_))
          case Bitop(Xor(d, k), h) =>
            client.bitop("XOR", d, k.list).map(h(_))
          case Bitop(Not(d, k), h) =>
            client.bitop("NOT", d, k).map(h(_))
          case Decr(k, h) =>
            client.decr(k).map(h(_))
          case Decrby(k, d, h) =>
            client.decrby(k, d.toInt).map(h(_))
          case Get(k, h) =>
            client.get(k).map(h(_))
          case Getbit(k, o, h) =>
            client.getbit(k, o).map(a => h(a.fold(1, 0)))
          case Getrange(k, s, t, h) =>
            client.getrange(k, s, t).map(a => h(a.cata(a => a, "")))
          case Getset(k, v, h) =>
            client.getset(k, v).map(h(_))
          case Incr(k, h) =>
            client.incr(k).map(h(_))
          case Incrby(k, i, h) =>
            client.incrby(k, i.toInt).map(h(_))
          case Incrbyfloat(k, i, h) =>
            Future.failed(new Exception("Unsupported operation Incrbyfloat")).map(_ => h(0))
          case Mget(k, h) =>
            client.mget[String](k.list).map(a => h(k.map(a.get(_).flatMap(a => (a != null).option(a))).list))
          case Mset(p, h) =>
            client.mset(p.list:_*).map(a => h(a.fold(Ok, Error)))
          case Msetnx(p, h) =>
            client.msetnx(p.list:_*).map(h(_))
          case Psetex(k, i, v, h) =>
            client.psetex(k, i.toInt, v).map(a => h(a.fold(Ok, Error)))
          case Set(k, v, i, o, h) =>
            client.set(
              key = k,
              value = v,
              exORpx = i.map(_.fold(EX(_), PX(_))),
              nxORxx = o.map {
                case Nx => NX
                case Xx => XX
              }
            ).map(h(_))
          case Setbit(k, o, v, h) =>
            client.setbit(k, o, v == "1").map(h(_))
          case Setex(k, i, v, h) =>
            client.setex(k, i.toInt, v).map(a => h(a.fold(Ok, Error)))
          case Setnx(k, v, h) =>
            client.setnx(k, v).map(h(_))
          case Setrange(k, o, v, h) =>
            client.setrange(k, o, v).map(h(_))
          case Strlen(k, h) =>
            client.strlen(k).map(h(_))
        }
    }
}
