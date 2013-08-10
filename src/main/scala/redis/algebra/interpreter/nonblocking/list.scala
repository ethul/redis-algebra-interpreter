package redis
package algebra
package interpreter
package nonblocking

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import NonBlocking._, future._

trait NonBlockingListInstance {
  implicit def listAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[ListAlgebra] =
    new NonBlocking[ListAlgebra] {
      def runAlgebra[A](algebra: ListAlgebra[A], ops: RedisOps) =
        algebra match {
          case Blpop(k, t, h) =>
            ops.blpop[String, String](t, k.head, k.tail:_*).map(h(_))
          case Brpop(k, t, h) =>
            ops.brpop[String, String](t, k.head, k.tail:_*).map(h(_))
          case Brpoplpush(s, d, t, h) =>
            ops.brpoplpush(s, d, t).map(h(_))
          case Lindex(k, i, h) =>
            ops.lindex(k, i.toInt).map(h(_))
          case Linsert(k, o, p, v, h) =>
            Future.failed(new Exception("Unsupported operation Linsert")).map(a => h(None))
          case Llen(k, h) =>
            ops.llen(k).map(h(_))
          case Lpop(k, h) =>
            ops.lpop(k).map(h(_))
          case Lpush(k, v, h) =>
            ops.lpush(k, v.head, v.tail:_*).map(h(_))
          case Lpushx(k, v, h) =>
            ops.lpushx(k, v).map(h(_))
          case Lrange(k, s, t, h) =>
            ops.lrange(k, s.toInt, t.toInt).map(h(_))
          case Lrem(k, c, v, h) =>
            ops.lrem(k, c.toInt, v).map(h(_))
          case Lset(k, i, v, a) =>
            ops.lset(k, i.toInt, v).map(_ => a)
          case Ltrim(k, s, t, a) =>
            ops.ltrim(k, s.toInt, t.toInt).map(_ => a)
          case Rpop(k, h) =>
            ops.rpop(k).map(h(_))
          case Rpoplpush(s, d, h) =>
            ops.rpoplpush(s, d).map(h(_))
          case Rpush(k, v, h) =>
            ops.rpush(k, v.head, v.tail:_*).map(h(_))
          case Rpushx(k, v, h) =>
            ops.rpushx(k, v).map(h(_))
        }
    }
}
