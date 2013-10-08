package redis
package algebra
package interpreter
package nonblocking

import com.redis.RedisClient
import com.redis.protocol.RedisCommand

import akka.pattern.{ask => akkaAsk}
import akka.util.Timeout

import scala.reflect.ClassTag

import scalaz.syntax.Ops

sealed abstract class RedisClientOps extends Ops[RedisClient] {
  final def ask[A: ClassTag](command: RedisCommand[A])(implicit T: Timeout) =
    self.clientRef.ask(command).mapTo[A]
}

sealed abstract class AnyRefOps extends Ops[AnyRef] {
  final def command =
    self.getClass.getSimpleName.toUpperCase
}

trait ToRedisClientOps {
  implicit def ToRedisClientOpsFromRedisClient(a: RedisClient): RedisClientOps =
    new RedisClientOps { val self = a }
}

trait ToAnyRefOps {
  implicit def ToAnyRefOpsFromAnyRef[A <: AnyRef](a: A): AnyRefOps =
    new AnyRefOps { val self = a }
}
