package org.constellation.snapshotstreaming

import cats.effect.IO
import cats.effect.kernel.Ref

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.security.signature.Signed

import eu.timepit.refined.auto._
import fs2.Stream
import org.constellation.snapshotstreaming.data._
import org.constellation.snapshotstreaming.node.NodeService
import org.constellation.snapshotstreaming.opensearch.SnapshotDAO
import weaver.SimpleIOSuite

object SnapshotServiceSuite extends SimpleIOSuite {

  def mkProcessedSnapshotsService(ref: Ref[IO, ProcessedSnapshots]) = new ProcessedSnapshotsService[IO] {

    override def initialState(): Stream[IO, ProcessedSnapshots] = Stream.eval(ref.get)

    override def saveState(processedSnapshots: ProcessedSnapshots): Stream[IO, Unit] =
      Stream.eval(ref.set(processedSnapshots))

  }

  def mkNodeService(ordinals: List[Long]) = new NodeService[IO] {

    override def getSnapshots(
      initOrdinal: Long,
      additionalOrdinals: Seq[Long]
    ): fs2.Stream[IO, Signed[GlobalSnapshot]] = Stream.emits(ordinals.map(globalSnapshot(_)))

  }

  def mkSnapshotDAO() = new SnapshotDAO[IO] {
    var counter = 0

    override def sendSnapshotToOpenSearch(snapshot: Signed[GlobalSnapshot]): fs2.Stream[IO, Long] =
      if (snapshot.ordinal.value.value > 4 && counter < 1) {
        counter = counter + 1
        Stream.raiseError(new Exception("DAO ERROR!"))
      } else Stream.emit(snapshot.ordinal.value.value)

  }

  def mkSnapshotService(
    nodeService: NodeService[IO],
    dao: SnapshotDAO[IO],
    processedService: ProcessedSnapshotsService[IO]
  ) =
    SnapshotService.make[IO, Signed[GlobalSnapshot]](nodeService, dao, processedService)

  test("process properly without gaps") {
    {
      for {
        ref <- Stream.eval(Ref[IO].of(ProcessedSnapshots(0, Nil)))
        _ <- mkSnapshotService(mkNodeService(List(0, 1, 2, 3)), mkSnapshotDAO(), mkProcessedSnapshotsService(ref))
          .processSnapshot()
      } yield ref
    }.take(5)
      .compile
      .toList
      .flatMap(l => l.head.get)
      .map(res => expect.same(res, ProcessedSnapshots(3, Nil)))
  }

  test("process properly with gaps") {
    {
      for {
        ref <- Stream.eval(Ref[IO].of(ProcessedSnapshots(0, Nil)))
        _ <- mkSnapshotService(mkNodeService(List(2, 3)), mkSnapshotDAO(), mkProcessedSnapshotsService(ref))
          .processSnapshot()
      } yield ref
    }.take(5)
      .compile
      .toList
      .flatMap(l => l.head.get)
      .map(res => expect.same(res, ProcessedSnapshots(3, List(1))))
  }

  test("should not fail when sending to opensearch fails") {
    {
      for {
        ref <- Stream.eval(Ref[IO].of(ProcessedSnapshots(0, Nil)))
        _ <- mkSnapshotService(mkNodeService(List(5)), mkSnapshotDAO(), mkProcessedSnapshotsService(ref))
          .processSnapshot()
      } yield ref
    }.take(1)
      .compile
      .toList
      .flatMap(l => l.head.get)
      .map(res => expect.same(res, ProcessedSnapshots(0, Nil)))
  }

  test("should not fail when sending and start from beginning to opensearch fails") {
    {
      for {
        ref <- Stream.eval(Ref[IO].of(ProcessedSnapshots(0, Nil)))
        _ <- mkSnapshotService(mkNodeService(List(5)), mkSnapshotDAO(), mkProcessedSnapshotsService(ref))
          .processSnapshot()
      } yield ref
    }.take(3)
      .compile
      .toList
      .flatMap(l => l.head.get)
      .map(res => expect.same(res, ProcessedSnapshots(5, List(1, 2, 3, 4))))
  }

}
