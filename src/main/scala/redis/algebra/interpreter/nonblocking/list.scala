package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.syntax.std.boolean._

import future._

trait NonBlockingListInstance extends ListInstances {
  implicit def listAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[ListAlgebra] =
    new NonBlocking[ListAlgebra] {
      def runAlgebra[A](algebra: ListAlgebra[A], client: RedisClient) =
        algebra match {
          case Blpop(k, t, h) =>
            client.blpop[String](t.toInt, k.list).map(h(_))
          case Brpop(k, t, h) =>
            client.brpop[String](t.toInt, k.list).map(h(_))
          case Brpoplpush(s, d, t, h) =>
            client.brpoplpush(s, d, t.toInt).map(h(_))
          case Lindex(k, i, h) =>
            client.lindex(k, i.toInt).map(h(_))
          case Linsert(k, o, p, v, h) =>
            Future.failed(new Exception("Unsupported operation Linsert")).map(a => h(None))
          case Llen(k, h) =>
            client.llen(k).map(h(_))
          case Lpop(k, h) =>
            client.lpop(k).map(h(_))
          case Lpush(k, v, h) =>
            client.lpush(k, v.head, v.tail:_*).map(h(_))
          case Lpushx(k, v, h) =>
            client.lpushx(k, v).map(h(_))
          case Lrange(k, s, t, h) =>
            client.lrange(k, s.toInt, t.toInt).map(h(_))
          case Lrem(k, c, v, h) =>
            client.lrem(k, c.toInt, v).map(h(_))
          case Lset(k, i, v, h) =>
            client.lset(k, i.toInt, v).map(a => h(a.fold(Ok, Error)))
          case Ltrim(k, s, t, h) =>
            client.ltrim(k, s.toInt, t.toInt).map(a => h(a.fold(Ok, Error)))
          case Rpop(k, h) =>
            client.rpop(k).map(h(_))
          case Rpoplpush(s, d, h) =>
            client.rpoplpush(s, d).map(h(_))
          case Rpush(k, v, h) =>
            client.rpush(k, v.head, v.tail:_*).map(h(_))
          case Rpushx(k, v, h) =>
            client.rpushx(k, v).map(h(_))
        }
    }
}
