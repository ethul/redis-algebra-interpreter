package redis
package algebra
package interpreter
package nonblocking

import org.scalacheck._, Arbitrary._, Gen._

import scalaz.NonEmptyList, NonEmptyList.nel

trait ArbitraryInstances {
  val arbArrayByteNonEmpty: Arbitrary[Array[Byte]] = Arbitrary(listOf1(arbitrary[Byte]).map(_.toArray))

  implicit val arbByteString: Arbitrary[ByteString] = Arbitrary(arbArrayByteNonEmpty.arbitrary.map(_.toIndexedSeq))

  implicit def arbByteStringNEL[A: Arbitrary]: Arbitrary[NonEmptyList[A]] =
    Arbitrary {
      for {
        n <- Gen.choose(0, 7)
        h <- arbitrary[A]
        t <- listOfN(n, arbitrary[A])
      } yield nel(h, t)
    }
}
