package redis
package algebra
package interpreter
package nonblocking

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.syntax.std.boolean._

import NonBlocking._, future._

trait NonBlockingHashInstance {
  implicit def hashAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[HashAlgebra] =
    new NonBlocking[HashAlgebra] {
      def runAlgebra[A](algebra: HashAlgebra[A], ops: RedisOps) =
        algebra match {
          case Hdel(k, f, h) =>
            ops.hdel(k, f.list).map(h(_))
          case Hexists(k, f, h) =>
            ops.hexists(k, f).map(h(_))
          case Hget(k, f, h) =>
            ops.hget(k, f).map(h(_))
          case Hgetall(k, h) =>
            ops.hgetall(k).map(a => h(a.toSeq))
          case Hincrby(k, f, i, h) =>
            ops.hincrby(k, f, i.toInt).map(h(_))
          case Hincrbyfloat(k, f, i, h) =>
            Future.failed(new Exception("Unsupported operation Hincrbyfloat")).map(_ => h(0))
          case Hkeys(k, h) =>
            ops.hkeys(k).map(h(_))
          case Hlen(k, h) =>
            ops.hlen(k).map(h(_))
          case Hmget(k, f, h) =>
            ops.hmget[String](k, f.list:_*).map(a => h(f.map(a.get(_).flatMap(a => (a != null).option(a))).list))
          case Hmset(k, p, a) =>
            ops.hmset(k, p.list).map(_ => a)
          case Hset(k, f, v, h) =>
            ops.hset(k, f, v).map(h(_))
          case Hsetnx(k, f, v, h) =>
            ops.hsetnx(k, f, v).map(h(_))
          case Hvals(k, h) =>
            ops.hvals(k).map(h(_))
        }
    }
}
