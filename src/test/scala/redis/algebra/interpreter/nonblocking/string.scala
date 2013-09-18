package redis
package algebra
package interpreter
package nonblocking

import org.specs2._, specification.{After, BeforeAfter}

import scalaz.{-\/, \/-, NonEmptyList}, NonEmptyList._
import scalaz.std.option._
import scalaz.syntax.{monad, zip}, monad._, zip._
import scalaz.syntax.std.option._

import all._

class NonBlockingStringInstanceSpec extends Specification with InterpreterSpec { def is = s2"""
  This is the specification for the string instance.

  Interpreting the bitop command with the and operation should
    result in storing the bitwise and of the strings                               ${ea().e1}

  Interpreting the bitop command with the or operation should
    result in storing the bitwise or of the strings                                ${ea().e2}

  Interpreting the bitop command with the xor operation should
    result in storing the bitwise xor of the strings                               ${ea().e3}

  Interpreting the bitop command with the not operation should
    result in storing the bitwise not of the string                                ${ea().e4}

  Interpreting the mget command with some existing keys should
    result in a list of optional values                                            ${eb().e5}

  Interpreting the set command with only a key and value should
    result in updating the value at that key                                       ${ec().e6}

  Interpreting the set command with a key, value, seconds, and nx should
    result in updating the value at that key with an expiration                    ${ec().e7}

  Interpreting the set command with an existing key and nx should
    result in not updating the value at that key                                   ${ec().e8}

  Interpreting the set command with an existing key, value, millis, and xx should
    result in updating the value at that key with an expiration                    ${ec().e9}

  Interpreting the set command with a key, value, millis, and xx should
    result in not updating the value at that key                                   ${ec().e10}
  """

  case class ea() extends BeforeAfter {
    def before = run(set[R](key1, "a1") >> set[R](key2, "a2") >> set[R](key3, "z"))

    def after = run(del(nels(key1, key2, key3, key4)))

    def e1 = this { run(bitop[R](And(key4, nels(key1, key2))) >> get[R](key4)) must beSome("a0") }

    def e2 = this { run(bitop[R](Or(key4, nels(key1, key2))) >> get[R](key4)) must beSome("a3") }

    def e3 = this { run(bitop[R](Xor(key4, nels(key1, key2))) >> get[R](key4)) must beSome("\0\3") }

    def e4 = this { run(bitop[R](Not(key4, key3)) >> get[R](key4)) must beSome(s"${0x0085.toChar}") }

    val (key1, key2, key3, key4) = (generate, generate, generate, generate)
  }

  case class eb() extends BeforeAfter {
    def before = run(set[R](key1, value1) >> set[R](key2, value2) >> set[R](key3, value3))

    def after = run(del(nels(key1, key2, key3)))

    def e5 = this { run(mget(nels(key1, key2, key3, key4))) === List(value1.some, value2.some, value3.some, None) }

    val (key1, key2, key3, key4) = (generate, generate, generate, generate)

    val (value1, value2, value3) = ("a", "b", "c")
  }

  case class ec() extends After {
    def after = run(del(nels(key)))

    def e6 = this { run(set[R](key, value1) >> get[R](key)) must beSome(value1) }

    def e7 = this { run {
      set[R](key, value1, -\/(expiration).some, Nx.some) >>
      get[R](key) >>= { a =>
        ttl[R](key).map { b =>
          a.fzip(b)
        }
      }
    } must beSome((value1, expiration)) }

    def e8 = this { run {
      set[R](key, value1) >>
      set[R](key, value2, -\/(expiration).some, Nx.some) >>
      get[R](key)
    } must beSome(value1) }

    def e9 = this { run {
      set[R](key, value1) >>
      set[R](key, value2, \/-(expiration).some, Xx.some) >>
      get[R](key) >>= { a =>
        ttl[R](key).map { b =>
          a.fzip(b)
        }
      }
    } must beSome((value2, expiration / 1000L)) }

    def e10 = this { run { set[R](key, value2, \/-(expiration).some, Xx.some) >> get[R](key) } must beNone }

    val key = generate

    val (value1, value2) = ("a", "b")

    val expiration = 10000L
  }
}
