package redis
package algebra
package interpreter
package nonblocking

import org.specs2._, specification.Before

import akka.util.{ByteString => AkkaByteString}

import scalaz.{-\/, \/-, NonEmptyList}, NonEmptyList._
import scalaz.std.option._
import scalaz.syntax.{monad, zip}, monad._, zip._
import scalaz.syntax.std.option._

import algebra.{all => r}, r._, algebra.data.{And, Error, Not, Nx, Ok, Or, Xor, Xx}

class NonBlockingStringInstanceSpec extends InterpreterSpecification with ArbitraryInstances with ScalaCheck { def is = s2"""
  This is the specification for the string instance.

  Interpreting the bitop command with the and operation should
    result in storing the bitwise and of the strings                               ${ebitop().e1}

  Interpreting the bitop command with the or operation should
    result in storing the bitwise or of the strings                                ${ebitop().e2}

  Interpreting the bitop command with the xor operation should
    result in storing the bitwise xor of the strings                               ${ebitop().e3}

  Interpreting the bitop command with the not operation should
    result in storing the bitwise not of the string                                ${ebitop().e4}
  """

  /*
  Interpreting the mget command with some existing keys should
    result in a list of optional values                                            ${emget().e5}

  Interpreting the set command with only a key and value should
    result in updating the value at that key                                       ${eset().e6}

  Interpreting the set command with a key, value, seconds, and nx should
    result in updating the value at that key with an expiration                    ${eset().e7}

  Interpreting the set command with an existing key and nx should
    result in not updating the value at that key                                   ${eset().e8}

  Interpreting the set command with an existing key, value, millis, and xx should
    result in updating the value at that key with an expiration                    ${eset().e9}

  Interpreting the set command with a key, value, millis, and xx should
    result in not updating the value at that key                                   ${eset().e10}
    */

  case class ebitop() {
    def e1 =
      prop { (a: ByteString, b: NonEmptyList[(ByteString, ByteString)]) =>
        val (c,d) = b.unzip
        val dest = genkey(a)
        val ks = c.map(genkey(_))
        run {
          mset[R](ks.zip(d)) >>
          bitop[R](And(dest, ks)) >>
          get[R](dest)
        } must beSome(op(d, _ & _))
      }

    def e2 =
      prop { (a: ByteString, b: NonEmptyList[(ByteString, ByteString)]) =>
        val (c,d) = b.unzip
        val dest = genkey(a)
        val ks = c.map(genkey(_))
        run {
          mset[R](ks.zip(d)) >>
          bitop[R](Or(dest, ks)) >>
          get[R](dest)
        } must beSome(op(d, _ | _))
      }

    def e3 =
      prop { (a: ByteString, b: NonEmptyList[(ByteString, ByteString)]) =>
        val (c,d) = b.unzip
        val dest = genkey(a)
        val ks = c.map(genkey(_))
        run {
          mset[R](ks.zip(d)) >>
          bitop[R](Xor(dest, ks)) >>
          get[R](dest)
        } must beSome(op(d, _ ^ _))
      }

    def e4 =
      prop { (a: ByteString, b: ByteString, c: ByteString) =>
        val dest = genkey(a)
        val k = genkey(b)
        val res = c.map(~_.toByte)
        run {
          r.set[R](k, c) >>
          bitop[R](Not(dest, k)) >>
          get[R](dest)
        } must beSome(res)
      }

    def op(as: NonEmptyList[ByteString], f: (Byte, Byte) => Int) =
      as.map(a => AkkaByteString(a.toArray).padTo(as.list.maxBy(_.size).size, 0.toByte).toArray).list.
        transpose.map(_.reduce(f(_, _).toByte))
  }

  case class emget() extends Before {
    def before = run(r.set[R](key1, value1) >> r.set[R](key2, value2) >> r.set[R](key3, value3))

    def e5 = this { run(mget(nels(key1, key2, key3, key4))) === List(value1.some, value2.some, value3.some, None) }

    val (key1, key2, key3, key4) = (generate, generate, generate, generate)

    val (value1, value2, value3) = ("a".utf8, "b".utf8, "c".utf8)
  }

  case class eset() {
    def e6 = run(r.set[R](key, value1) >> get[R](key)) must beSome(value1)

    def e7 = run {
      r.set[R](key, value1, -\/(expiration).some, Nx.some) >>
      get[R](key) >>= { a =>
        ttl[R](key).map { b =>
          a.fzip(b)
        }
      }
    } must beSome((value1, expiration))

    def e8 = run {
      r.set[R](key, value1) >>
      r.set[R](key, value2, -\/(expiration).some, Nx.some) >>
      get[R](key)
    } must beSome(value1)

    def e9 = run {
      r.set[R](key, value1) >>
      r.set[R](key, value2, \/-(expiration).some, Xx.some) >>
      get[R](key) >>= { a =>
        ttl[R](key).map { b =>
          a.fzip(b)
        }
      }
    } must beSome((value2, expiration / 1000L))

    def e10 = run { r.set[R](key, value2, \/-(expiration).some, Xx.some) >> get[R](key) } must beNone

    val key = generate

    val (value1, value2) = ("a".utf8, "b".utf8)

    val expiration = 10000L
  }
}
