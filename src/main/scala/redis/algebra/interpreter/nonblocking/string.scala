package redis
package algebra
package interpreter
package nonblocking

import akka.util.Timeout

import com.redis.protocol.StringCommands.{EX, PX, NX, XX}

import scala.concurrent.{ExecutionContext, Future}

import scalaz.{-\/, \/-}
import scalaz.std.option._
import scalaz.syntax.zip._
import scalaz.syntax.std.{boolean, option}, boolean._, option._

import NonBlocking._, future._

trait NonBlockingStringInstance {
  implicit def stringAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[StringAlgebra] =
    new NonBlocking[StringAlgebra] {
      def runAlgebra[A](algebra: StringAlgebra[A], ops: RedisOps) =
        algebra match {
          case Append(k, v, h) =>
            ops.append(k, v).map(h(_))
          case Bitcount(k, s, e, h) =>
            ops.bitcount(k, s.fzip(e)).map(h(_))
          case Bitop(And(d, k), h) =>
            ops.bitop("AND", d, k.list:_*).map(h(_))
          case Bitop(Or(d, k), h) =>
            ops.bitop("OR", d, k.list:_*).map(h(_))
          case Bitop(Xor(d, k), h) =>
            ops.bitop("XOR", d, k.list:_*).map(h(_))
          case Bitop(Not(d, k), h) =>
            ops.bitop("NOT", d, k).map(h(_))
          case Decr(k, h) =>
            ops.decr(k).map(h(_))
          case Decrby(k, d, h) =>
            ops.decrby(k, d.toInt).map(h(_))
          case Get(k, h) =>
            ops.get(k).map(h(_))
          case Getbit(k, o, h) =>
            ops.getbit(k, o).map(a => h(a.fold(1, 0)))
          case Getrange(k, s, t, h) =>
            ops.getrange(k, s, t).map(a => h(a.cata(a => a, "")))
          case Getset(k, v, h) =>
            ops.getset(k, v).map(h(_))
          case Incr(k, h) =>
            ops.incr(k).map(h(_))
          case Incrby(k, i, h) =>
            ops.incrby(k, i.toInt).map(h(_))
          case Incrbyfloat(k, i, h) =>
            Future.failed(new Exception("Unsupported operation Incrbyfloat")).map(_ => h(0))
          case Mget(k, h) =>
            ops.mget[String](k.list).map(a => h(k.map(a.get(_).flatMap(a => (a != null).option(a))).list))
          case Mset(p, a) =>
            ops.mset(p.list:_*).map(_ => a)
          case Msetnx(p, h) =>
            ops.msetnx(p.list:_*).map(h(_))
          case Psetex(k, i, v, a) =>
            ops.psetex(k, i.toInt, v).map(_ => a)
          case Set(k, v, i, o, h) =>
            ops.set(
              key = k,
              value = v,
              exORpx = i.map(_.fold(EX(_), PX(_))),
              nxORxx = o.map {
                case Nx => NX
                case Xx => XX
              }
            ).map(h(_))
          case Setbit(k, o, v, h) =>
            ops.setbit(k, o, v == "1").map(h(_))
          case Setex(k, i, v, a) =>
            ops.setex(k, i.toInt, v).map(_ => a)
          case Setnx(k, v, h) =>
            ops.setnx(k, v).map(h(_))
          case Setrange(k, o, v, h) =>
            ops.setrange(k, o, v).map(h(_))
          case Strlen(k, h) =>
            ops.strlen(k).map(h(_))
        }
    }
}
