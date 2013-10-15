package redis
package algebra
package interpreter
package nonblocking

import org.specs2._, specification.Before

import scalaz.NonEmptyList, NonEmptyList._
import scalaz.std.list._
import scalaz.syntax.{monad, traverse}, monad._, traverse._
import scalaz.syntax.std.{list, option}, list._, option._

import algebra.{all => r}, r._

class NonBlockingSetInstanceSpec extends InterpreterSpecification with ArbitraryInstances with ScalaCheckSpecification { def is = s2"""
  This is the specification for the set instance.

  Interpreting the sadd command should
    result in adding the value to the set at the given key             ${esadd().e1}

  Interpreting the sismember command should
    result in true when the value is a set member                      ${esismember().e1}

  Interpreting the sremove command should
    result in removing those values from the set                       ${esrem().e1}
  """

  case class esadd() {
    def e1 =
      prop { (a: ByteString, b: NonEmptyList[ByteString]) =>
        val k = genkey(a)
        run(sadd[R](k, b) >>= (x => smembers[R](k).map((x, _)))) === ((b.list.toSet.size, b.list.toSet))
      }
  }

  case class esismember() {
    def e1 =
      prop { (a: ByteString, b: NonEmptyList[ByteString], c: NonEmptyList[ByteString]) =>
        val k = genkey(a)
        run(sadd[R](k, b) >> (b.append(c)).map(x => sismember[R](k, x)).list.sequenceU) ===
          (b.map(_ => true).append(c.map(x => b.list.exists(_ == x)))).list
      }
  }

  case class esrem() {
    def e1 =
      prop { (a: ByteString, b: NonEmptyList[ByteString], c: NonEmptyList[ByteString]) =>
        val k = genkey(a)
        run(sadd[R](k, b.append(c)) >> srem[R](k, b) >> smembers[R](k)) === ((b.append(c)).list.toSet -- b.list.toSet)
      }
  }
}
