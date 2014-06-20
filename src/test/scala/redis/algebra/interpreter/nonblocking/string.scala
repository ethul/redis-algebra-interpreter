package redis
package algebra
package interpreter
package nonblocking

import org.specs2._, specification.Before
import org.scalacheck.Arbitrary

import akka.util.{ByteString => AkkaByteString}

import scalaz.{-\/, \/-, NonEmptyList}, NonEmptyList._
import scalaz.std.option._
import scalaz.syntax.{monad, zip}, monad._, zip._
import scalaz.syntax.std.option._

import algebra.{all => r}, r._, algebra.data.{And, Error, Not, Nx, Ok, Or, Xor, Xx}

class NonBlockingStringInstanceSpec extends InterpreterSpecification with ArbitraryInstances with ScalaCheckSpecification { def is = s2"""
  This is the specification for the string instance.

  Interpreting the append command should
    result in the value appendeded to the value of the string key                  ${eappend().e1}

  Interpreting the bitop command with the and operation should
    result in storing the bitwise and of the strings                               ${ebitop().e1}

  Interpreting the bitop command with the or operation should
    result in storing the bitwise or of the strings                                ${ebitop().e2}

  Interpreting the bitop command with the xor operation should
    result in storing the bitwise xor of the strings                               ${ebitop().e3}

  Interpreting the bitop command with the not operation should
    result in storing the bitwise not of the string                                ${ebitop().e4}

  Interpreting the decr command should
    result in decrementing the value of the key by one                             ${edecr().e1}

  Interpreting the decrby command should
    result in decrementing the value of the key by the provided value              ${edecrby().e1}

  Interpreting the get command should
    result optionally in the value for the given key                               ${eget().e1}

  Interpreting the getset command should
    result optionally in the old value for the given key                           ${egetset().e1}

  Interpreting the set command with a value should
    result in the value being stored at the given key                              ${eset().e1}

  Interpreting the set command with a value and ttl in seconds should
    result in the value being stored at the given key with a ttl                   ${eset().e2}

  Interpreting the set command with a value and ttl in milliseconds should
    result in the value being stored at the given key with a ttl                   ${eset().e3}

  Interpreting the set command with Nx or XX options should
    result in the value being stored depending on the current value at the key     ${eset().e4}
  """

  case class eappend() {
    def e1 =
      prop { (a: ByteString, b: ByteString, c: ByteString) =>
        val key = genkey(a)
        run {
          r.set[R](key, b) >>
          append[R](key, c) >>=
          (x => get[R](key).map(_.map((x, _))))
        } must beSome((b.size + c.size, b ++ c))
      }
  }

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

  case class edecr() {
    def e1 =
      prop { (a: ByteString, b: Int) =>
        val k = genkey(a)
        run(r.set[R](k, b.utf8) >> decr[R](k)) === b.toLong - 1L
      }
  }

  case class edecrby() {
    def e1 =
      prop { (a: ByteString, b: Int, c: Int) =>
        val k = genkey(a)
        run(r.set[R](k, b.utf8) >> decrby[R](k, c)) === b.toLong - c.toLong
      }
  }

  case class eget() {
    implicit val A0: Arbitrary[(ByteString, ByteString)] = arbByteStringSometimesMatchingPair

    def e1 =
      prop { (a: (ByteString, ByteString), b: ByteString) =>
        val (a0, a1) = a
        val ks = genkeys(nels(a0, a1))
        run(r.set[R](ks.head, b) >> get[R](ks.tail.head)) must (if (a0 == a1) beSome(b) else beNone)
      }
  }

  case class egetset() {
    implicit val A0: Arbitrary[(ByteString, ByteString)] = arbByteStringSometimesMatchingPair

    def e1 =
      prop { (a: (ByteString, ByteString), b: ByteString, c: ByteString) =>
        val (a0, a1) = a
        val ks = genkeys(nels(a0, a1))
        run(r.set[R](ks.head, b) >> getset[R](ks.tail.head, c) >>= (x => get[R](ks.tail.head).map((x, _)))) ===
          (if (a0 == a1) (Some(b), Some(c)) else (None, Some(c)))
      }
  }

  case class eset() {
    def e1 =
      prop { (a: ByteString, b: ByteString) =>
        val k = genkey(a)
        run(r.set[R](k, b) >> get[R](k)) must beSome(b)
      }

    def e2 = {
      implicit val A0: Arbitrary[Seconds] = arbSeconds
      prop { (a: ByteString, b: ByteString, e: Seconds) =>
        val k = genkey(a)
        run(r.set[R](k, b, -\/(e).some) >> get[R](k) >>= (x => ttl[R](k).map((x, _)))) === ((b.some, e.some))
      }
    }

    def e3 = {
      implicit val A0: Arbitrary[Milliseconds] = Arbitrary(arbSeconds.arbitrary.map(_ * 1000L))
      prop { (a: ByteString, b: ByteString, e: Milliseconds) =>
        val k = genkey(a)
        run(r.set[R](k, b, \/-(e).some) >> get[R](k) >>= (x => ttl[R](k).map((x, _)))) === ((b.some, (e / 1000L).some))
      }
    }

    def e4 = {
      implicit val A0: Arbitrary[Seconds] = arbSeconds
      prop { (a: ByteString, b: ByteString, c: ByteString, e: Seconds) =>
        val k = genkey(a)
        run {
          r.set[R](k, b, -\/(e).some, Nx.some) >>
          r.set[R](k, c, None, Xx.some) >>
          get[R](k) >>=
          (x => ttl[R](k).map((x, _)))
        } === ((c.some, None))
      }
    }
  }
}
