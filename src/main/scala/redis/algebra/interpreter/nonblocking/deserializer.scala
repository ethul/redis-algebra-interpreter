package redis
package algebra
package interpreter
package nonblocking

import com.redis.serialization.PartialDeserializer

import akka.util.{ByteString => AkkaByteString}

import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

trait PartialDeserializerInstances {
  implicit def OptionLongDeserializer
    (implicit P0: PartialDeserializer[Long],
              P1: PartialDeserializer[Option[AkkaByteString]]): PartialDeserializer[Option[Long]] =
      P0.andThen(_.some).orElse(P1.andThen(_ >> none))
}
