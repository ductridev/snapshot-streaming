package org.constellation.snapshotstreaming

import cats.Show
import cats.effect.IO
import cats.kernel.Order
import cats.syntax.contravariant._

import scala.collection.mutable.{Map, TreeMap}

import org.tessellation.dag.snapshot.{GlobalSnapshot, SnapshotOrdinal => OriginalSnapshotOrdinal}
import org.tessellation.security.signature.Signed

import eu.timepit.refined.auto._
import fs2.Stream
import org.constellation.snapshotstreaming.data._
import org.constellation.snapshotstreaming.node.{NodeDownload, NodeService, SnapshotOrdinal}
import org.scalacheck.Gen
import weaver.SimpleIOSuite
import weaver.scalacheck.Checkers

object NodeServiceSuite extends SimpleIOSuite with Checkers {

  case class TestSnapshot(ordinal: SnapshotOrdinal)

  def mkNodeDownload(ordinals: List[SnapshotOrdinal], ordinalsFromOtherNodes: List[SnapshotOrdinal] = Nil) =
    new NodeDownload[IO] {

      val callsPerKnownOrdinal: Map[SnapshotOrdinal, Int] = TreeMap.from(ordinalsFromOtherNodes.map(s => (s, 0)))
      val limitToCallsForOrdinal = 1
      def downloadLastOrdinal(): Stream[IO, SnapshotOrdinal] = Stream.emits(ordinals)

      def downloadSnapshot(ordinal: SnapshotOrdinal): Stream[IO, Either[SnapshotOrdinal, Signed[GlobalSnapshot]]] =
        if (ordinals.contains(ordinal))
          Stream(Right(globalSnapshot(ordinal.value)))
        else if (callsPerKnownOrdinal.updateWith(ordinal)(_.map(_ + 1)).exists(_ > limitToCallsForOrdinal))
          Stream.raiseError(new Exception("TEST ERROR"))
        else
          Stream(Left(ordinal))

    }

  def mkNodeService(nodeDownloadPool: List[NodeDownload[IO]]) =
    NodeService.make[IO](nodeDownloadPool)

  test("process all ordinals") {
    val expected = List[Long](1, 2, 3)
    val actual = mkNodeService(List(mkNodeDownload(List(0, 1, 2, 3).map(SnapshotOrdinal(_))))).getSnapshots(0, Nil)
    actual.assert(expected)
  }

  test("process all ordinals starting from from initial") {
    val expected = List[Long](3, 4)
    val actual = mkNodeService(List(mkNodeDownload(List(1, 2, 3, 4).map(SnapshotOrdinal(_))))).getSnapshots(2, Nil)
    actual.assert(expected)
  }

  test("process all ordinals starting from from initial and also from gaps") {
    val expected = List[Long](2, 5, 8, 9)
    val actual = mkNodeService(List(mkNodeDownload(List(1, 2, 3, 4, 5, 6, 7, 8, 9).map(SnapshotOrdinal(_)))))
      .getSnapshots(7, List(2, 5))
    actual.assert(expected)
  }

  test("skip errored ordinal") {
    val expected = List[Long](1, 2, 4, 9)
    val actual = mkNodeService(List(mkNodeDownload(List(1, 2, 4, 9).map(SnapshotOrdinal(_))))).getSnapshots(0, Nil)
    actual.assert(expected)
  }

  test("all snapshots downloaded from different nodes") {
    forall(ordinalsPerNodeGen) { ordinalsPerNode: List[List[SnapshotOrdinal]] =>
      val expected: List[Long] =
        ordinalsPerNode.flatten.distinct.map(ordinal => ordinal.value).sorted

      val nodeDownloads = ordinalsPerNode.map(ordinals => mkNodeDownload(ordinals))
      val actual = mkNodeService(nodeDownloads).getSnapshots(0, Nil)

      actual.assert(expected)
    }
  }

  implicit class AssertGlobalSnapshotStream(s: Stream[IO, Signed[GlobalSnapshot]]) {

    def assert(expected: List[Long]) =
      s
        .take(expected.size)
        .compile
        .toList
        .map(_.map(_.ordinal.value.value).sorted)
        .map(actual => expect.same(actual, expected))

  }

  implicit val snapshotOrdinalShow: Show[SnapshotOrdinal] = Show[Long].contramap(_.value)
  implicit val order: Order[Signed[GlobalSnapshot]] = Order[OriginalSnapshotOrdinal].contramap(_.ordinal)
  implicit val ordering: Ordering[Signed[GlobalSnapshot]] = order.toOrdering

  val snapshotOrdinalGen = Gen.chooseNum(1, 22).map(SnapshotOrdinal(_))

  val ordinalsPerNodeGen: Gen[List[List[SnapshotOrdinal]]] = for {
    numberOfNodes <- Gen.choose(1, 3)
    nodesOrdinals <- Gen.listOfN(numberOfNodes, Gen.nonEmptyListOf(snapshotOrdinalGen))
  } yield nodesOrdinals

}
