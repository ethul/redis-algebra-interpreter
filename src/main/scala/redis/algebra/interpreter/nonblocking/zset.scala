package redis
package algebra
package interpreter
package nonblocking

import com.redis.protocol.RedisCommand.{ASC, DESC, MAX, MIN, SUM}

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.std.tuple._
import scalaz.syntax.all._
import scalaz.syntax.std.{boolean, option}, boolean._, option._

import NonBlocking._, future._

trait NonBlockingZSetInstance {
  implicit def zsetAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[ZSetAlgebra] =
    new NonBlocking[ZSetAlgebra] {
      def runAlgebra[A](algebra: ZSetAlgebra[A], ops: RedisOps) =
        algebra match {
          case Zadd(k, p, h) =>
            val (s, v) = p.head
            ops.zadd(k, s, v, p.tail:_*).map(h(_))
          case Zcard(k, h) =>
            ops.zcard(k).map(h(_))
          case Zcount(k, m, n, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            ops.zcount(k, v, w, d, d).map(h(_))
          // TODO: Why does this return an option double?
          case Zincrby(k, i, m, h) =>
            ops.zincrby(k, i, m).map(a => a.cata(h(_), h(i)))
          // TODO: Do we need the numkeys?
          case Zinterstore(d, _, k, None, a, h) =>
            ops.zinterstore(d, k.list, aggregate(a)).map(h(_))
          case Zinterstore(d, _, k, Some(w), a, h) =>
            ops.zinterstoreweighted(d, k.zip(w).list, aggregate(a)).map(h(_))
          case Zrange(k, s, t, false, h) =>
            ops.zrange(k, s.toInt, t.toInt, ASC).map(a => h(a.map((_, None))))
          case Zrange(k, s, t, true, h) =>
            ops.zrangeWithScore(k, s.toInt, t.toInt, ASC).map(a => h(a.map(_.map(_.some))))
          case Zrangebyscore(k, m, n, false, l, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            ops.zrangeByScore(k, v, c, w, d, l.map(a => (a.offset.toInt, a.count.toInt)), ASC).map(a => h(a.map((_, None))))
          case Zrangebyscore(k, m, n, true, l, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            ops.zrangeByScoreWithScore(k, v, c, w, d, l.map(a => (a.offset.toInt, a.count.toInt)), ASC).map(a => h(a.map(_.map(_.some))))
          case Zrank(k, m, h) =>
            ops.zrank(k, m, false).map(a => h((a == null).option(a)))
          case Zrem(k, m, h) =>
            ops.zrem(k, m).map(h(_))
          case Zremrangebyrank(k, s, t, h) =>
            ops.zremrangebyrank(k, s.toInt, t.toInt).map(h(_))
          case Zremrangebyscore(k, s, t, h) =>
            val (v, _) = endpoint(s)
            val (w, _) = endpoint(t)
            ops.zremrangebyscore(k, v, w).map(h(_))
          case Zrevrange(k, s, t, false, h) =>
            ops.zrange(k, s.toInt, t.toInt, DESC).map(a => h(a.map((_, None))))
          case Zrevrange(k, s, t, true, h) =>
            ops.zrangeWithScore(k, s.toInt, t.toInt, DESC).map(a => h(a.map(_.map(_.some))))
          case Zrevrangebyscore(k, m, n, false, l, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            ops.zrangeByScore(k, v, c, w, d, l.map(a => (a.offset.toInt, a.count.toInt)), DESC).map(a => h(a.map((_, None))))
          case Zrevrangebyscore(k, m, n, true, l, h) =>
            val (v, c) = endpoint(m)
            val (w, d) = endpoint(n)
            ops.zrangeByScoreWithScore(k, v, c, w, d, l.map(a => (a.offset.toInt, a.count.toInt)), DESC).map(a => h(a.map(_.map(_.some))))
          case Zrevrank(k, m, h) =>
            ops.zrank(k, m, true).map(a => h((a == null).option(a)))
          case Zscore(k, m, h) =>
            ops.zscore(k, m).map(h(_))
          case Zunionstore(d, _, k, None, a, h) =>
            ops.zunionstore(d, k.list, aggregate(a)).map(h(_))
          case Zunionstore(d, _, k, Some(w), a, h) =>
            ops.zunionstore(d, k.zip(w).list, aggregate(a)).map(h(_))
        }

      def endpoint(e: Endpoint) = e match {
        case Closed(v) => (v, true)
        case Open(v) => (v, false)
        case -∞ => (Double.NegativeInfinity, false)
        case +∞ => (Double.PositiveInfinity, false)
      }

      def aggregate(a: Aggregate) = a match {
        case Sum => SUM
        case Min => MIN
        case Max => MAX
      }
    }
}
