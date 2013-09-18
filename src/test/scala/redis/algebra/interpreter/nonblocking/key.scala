package redis
package algebra
package interpreter
package nonblocking

import org.specs2._, specification.BeforeAfter

import scalaz.{-\/, \/-, NonEmptyList}, NonEmptyList._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

import all._

class NonBlockingKeyInstanceSpec extends Specification with InterpreterSpec { def is = s2"""
  This is the specification for the key instance.

  Interpreting the pttl command with a key that does not exist should
    result in a none value                                             ${ea().e1}

  Interpreting the pttl command with a key that exists should
    result in a some value of the time to live in milliseconds         ${ea().e2}

  Interpreting the ttl command with a key that does not exist should
    result in a none value                                             ${ea().e3}

  Interpreting the ttl command with a key that exists should
    result in a some value of the time to live in seconds              ${ea().e4}

  Interpreting the type command with a key that has a string should
    result in a some RedisString object                                ${eb().e5}

  Interpreting the type command with a key that has a hash should
    result in a some RedisHash object                                  ${eb().e6}

  Interpreting the type command with a key that has a list should
    result in a some RedisList object                                  ${eb().e7}

  Interpreting the type command with a key that has a set should
    result in a some RedisSet object                                   ${eb().e8}

  Interpreting the type command with a key that has a zset should
    result in a some RedisZSet object                                  ${eb().e9}

  Interpreting the type command with a non existing key should
    result in a none                                                   ${eb().e10}

  Interpreting the sort command on a list should
    result in an sorted ascending list                                 ${ec().e11}

  Interpreting the sort command on a list with Desc should
    result in an sorted descending list                                ${ec().e12}

  Interpreting the sort command on a list with a key to store should
    result in the length of the list                                   ${ec().e13}

  Interpreting the sort command on a list with a key to store should
    result in the sorted ascending list at the store key               ${ec().e14}
  """

  case class ea() extends BeforeAfter {
    def before = run(set(key, "value", -\/(seconds).some))

    def after = run(del(nels(key)))

    def e1 = this { run(pttl(s"${key}${key}")) must beNone }

    def e2 = this { run(pttl(key)) must beSome.which(_ => true) }

    def e3 = this { run(ttl(s"${key}${key}")) must beNone }

    def e4 = this { run(ttl(key)) must beSome.which(_ => true) }

    val seconds = 100L

    val key = generate
  }

  case class eb() extends BeforeAfter {
    def before = run {
      set[R](key1, "value") >>
      hset[R](key2, "field", "value") >>
      lpush[R](key3, nels("value")) >>
      sadd[R](key4, nels("value")) >>
      zadd[R](key5, nels((1.0, "value")))
    }

    def after = run(del(nels(key1, key2, key3, key4, key5)))

    def e5 = this { run(`type`(key1)) must beSome(RedisString) }

    def e6 = this { run(`type`(key2)) must beSome(RedisHash) }

    def e7 = this { run(`type`(key3)) must beSome(RedisList) }

    def e8 = this { run(`type`(key4)) must beSome(RedisSet) }

    def e9 = this { run(`type`(key5)) must beSome(RedisZSet) }

    def e10 = this { run(`type`(key6)) must beNone }

    val (key1, key2, key3, key4, key5, key6) = (generate, generate, generate, generate, generate, generate)
  }

  case class ec() extends BeforeAfter {
    def before = run(lpush(key1, nel(unsorted.head.toString, unsorted.tail.map(_.toString))))

    def after = run(del(nels(key1, key2)))

    def e11 = this { run(sort(key1)) must beLike {
      case -\/(a) =>
        a must contain(allOf(unsorted.sorted.map(_.toString):_*).inOrder)
      case _ =>
        ko
    }}

    def e12 = this { run(sort(key = key1, order = Desc)) must beLike {
      case -\/(a) =>
        a must contain(allOf(unsorted.sorted.reverse.map(_.toString):_*).inOrder)
      case _ =>
        ko
    }}

    def e13 = this { run(sort(key = key1, store = key2.some)) must beLike {
      case \/-(a) =>
        a === unsorted.length
      case _ =>
        ko
    }}

    def e14 = this {
      run {
        sort[R](key = key1, store = key2.some) >>= {
          case \/-(a) => lrange[R](key2, 0, a)
          case _ => throw new Exception("Unexpected case")
        }
      } must contain(allOf(unsorted.sorted.map(_.toString):_*).inOrder)
    }

    val unsorted = List(10, 5, 3, 1, 9, 12)

    val (key1, key2) = (generate, generate)
  }
}
