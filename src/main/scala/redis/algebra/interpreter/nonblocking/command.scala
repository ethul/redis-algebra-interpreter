package redis
package algebra
package interpreter
package nonblocking

import com.redis.protocol.RedisCommand, RedisCommand.Args
import com.redis.serialization.PartialDeserializer

final case class Command[A: PartialDeserializer](name: String, args: Args)
  extends RedisCommand[A](name) { def params = args }
