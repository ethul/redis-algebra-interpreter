package redis
package algebra
package interpreter
package nonblocking

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.syntax.std.option._

import NonBlocking._, future._

trait NonBlockingSetInstance {
  implicit def setAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[SetAlgebra] =
    new NonBlocking[SetAlgebra] {
      def runAlgebra[A](algebra: SetAlgebra[A], ops: RedisOps) =
        algebra match {
          case Sadd(k, m, h) =>
            ops.sadd(k, m.head, m.tail:_*).map(h(_))
          case Scard(k, h) =>
            ops.scard(k).map(h(_))
          case Sdiff(k, h) =>
            ops.sdiff(k.head, k.tail:_*).map(h(_))
          case Sdiffstore(d, k, h) =>
            ops.sdiffstore(d, k.head, k.tail:_*).map(h(_))
          case Sinter(k, h) =>
            ops.sinter(k.head, k.tail:_*).map(h(_))
          case Sinterstore(d, k, h) =>
            ops.sinterstore(d, k.head, k.tail:_*).map(h(_))
          case Sismember(k, m, h) =>
            ops.sismember(k, m).map(h(_))
          case Smembers(k, h) =>
            ops.smembers(k).map(h(_))
          case Smove(s, d, m, h) =>
            ops.smove(s, d, m).map(a => h(a == 1L))
          case Spop(k, h) =>
            ops.spop(k).map(h(_))
          case Srandmember(k, c, h) =>
            ops.srandmember(k, c.cata(a => a.toInt, 1)).map(a => h(a.toSet))
          case Srem(k, m, h) =>
            ops.srem(k, m.head, m.tail:_*).map(h(_))
          case Sunion(k, h) =>
            ops.sunion(k.head, k.tail:_*).map(h(_))
          case Sunionstore(d, k, h) =>
            ops.sunionstore(d, k.head, k.tail:_*).map(h(_))
        }
    }
}
