package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient

import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

import scalaz.syntax.std.{boolean, option}, boolean._, option._

import future._

trait NonBlockingServerInstance extends ServerInstances {
  implicit def serverAlgebraNonBlocking(implicit EC: ExecutionContext, T: Timeout): NonBlocking[ServerAlgebra] =
    new NonBlocking[ServerAlgebra] {
      def runAlgebra[A](algebra: ServerAlgebra[A], client: RedisClient) =
        algebra match {
          case Bgrewriteaof(h) =>
            client.bgrewriteaof.map(a => h(a.fold(Ok, Error)))
          case Bgsave(h) =>
            client.bgsave.map(a => h(a.fold(Ok, Error)))
          case Clientgetname(h) =>
            client.client.getname.map(h(_))
          case Clientkill(i, p, h) =>
            client.client.kill(s"${i}:${p}").map(a => h(a.fold(Ok, Error)))
          case Clientlist(h) =>
            client.client.list.map(a => h(a.cata(_.split('\n').map(_.split(' ').map(_.split('=')).map(a => (a.head, a.last)).toMap).toList, Nil)))
          case Clientsetname(n, h) =>
            client.client.setname(n).map(a => h(a.fold(Ok, Error)))
          case Configget(p, h) =>
            client.config.get(p).map(a => h(a.cata(_.split('\n').toList, Nil)))
          case Configresetstat(h) =>
            client.config.resetstat.map(a => h(a.fold(Ok, Error)))
          case Configrewrite(h) =>
            client.config.rewrite.map(a => h(a.fold(Ok, Error)))
          case Configset(p, v, h) =>
            client.config.set(p, v).map(a => h(a.fold(Ok, Error)))
          case Dbsize(h) =>
            client.dbsize.map(a => h(a.toShort))
          case Debugobject(_, h) =>
            Future.failed(new Exception("Unsupported operation Debugobject")).map(_ => h(Error))
          case Debugsegfault(h) =>
            Future.failed(new Exception("Unsupported operation Debugsegfault")).map(_ => h(Error))
          case Flushall(h) =>
            client.flushall.map(a => h(a.fold(Ok, Error)))
          case Flushdb(h) =>
            client.flushdb.map(a => h(a.fold(Ok, Error)))
          case Info(_, h) =>
            client.info.map(a => h(a.cata(_.split('\n').filter(_.isEmpty).foldLeft(("", Map[String, Map[String, String]]())) { (b, line) =>
              line.startsWith("#").fold(
                (line.split('#').last.trim, b._2), {
                  val Array(k, v) = line.split(':')
                  val c = b._2(b._1) + (k -> v)
                  (b._1, b._2 + (b._1 -> c))
                }
              )
            }._2, Map())))
          case Lastsave(h) =>
            client.lastsave.map(h(_))
          case Monitor(h) =>
            Future.failed(new Exception("Unsupported operation Monitor")).map(_ => h(Stream.Empty))
          case Save(h) =>
            client.save.map(a => h(a.fold(Ok, Error)))
          case Shutdown(_, h) =>
            client.shutdown.map(a => h(a.fold(Ok, Error)))
          case Slaveof(Noone, h) =>
            client.slaveof(None).map(a => h(a.fold(Ok, Error)))
          case Slaveof(Host(n, p), h) =>
            client.slaveof(Some((n, p))).map(a => h(a.fold(Ok, Error)))
          case Slowlog(_, h) =>
            Future.failed(new Exception("Unsupported operation Slowlog")).map(_ => h(null))
          case Sync(a) =>
            Future.failed(new Exception("Unsupported operation Sync")).map(_ => a)
          case Time(h) =>
            Future.failed(new Exception("Unsupported operation Time")).map(_ => h((0L, 0)))
        }
    }
}
