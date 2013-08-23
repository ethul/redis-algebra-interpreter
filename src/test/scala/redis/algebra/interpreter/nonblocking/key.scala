package redis
package algebra
package interpreter
package nonblocking

import org.specs2._, specification.BeforeAfter

import scalaz.{-\/, NonEmptyList}, NonEmptyList._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

import HashAlgebra._, KeyAlgebra._, ListAlgebra._, SetAlgebra._, StringAlgebra._, ZSetAlgebra._

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
    result in a string_ type object                                    ${eb().e5}

  Interpreting the type command with a key that has a hash should
    result in a hash_ type object                                      ${eb().e6}

  Interpreting the type command with a key that has a list should
    result in a list_ type object                                      ${eb().e7}

  Interpreting the type command with a key that has a set should
    result in a set_ type object                                       ${eb().e8}

  Interpreting the type command with a key that has a zset should
    result in a zset_ type object                                      ${eb().e9}
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

    def e5 = this { run(type_(key1)) === string_ }

    def e6 = this { run(type_(key2)) === hash_ }

    def e7 = this { run(type_(key3)) === list_ }

    def e8 = this { run(type_(key4)) === set_ }

    def e9 = this { run(type_(key5)) === zset_ }

    val (key1, key2, key3, key4, key5) = (generate, generate, generate, generate, generate)
  }
}
