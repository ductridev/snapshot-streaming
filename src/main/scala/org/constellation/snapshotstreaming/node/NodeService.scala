package org.constellation.snapshotstreaming.node

import cats.effect.kernel.Async
import cats.effect.std.Queue
import cats.syntax.traverse._

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.security.signature.Signed

import fs2.{Pipe, Stream}
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait NodeService[F[_]] {
  def getSnapshots(initialOrdinal: Long, additionalOrdinals: Seq[Long]): Stream[F, Signed[GlobalSnapshot]]
}

object NodeService {

  def make[F[_]: Async](nodeDownloadsPool: Seq[NodeDownload[F]]) = new NodeService[F] {

    private val logger = Slf4jLogger.getLoggerFromClass[F](NodeService.getClass)
    val nodePoolSize = nodeDownloadsPool.size
    val snapshotOrdinalStreamsPool = nodeDownloadsPool.map(nodeDownload => nodeDownload.downloadLastOrdinal())
    val snapshotPipesPool = nodeDownloadsPool.zipWithIndex.map((downloadPipe _).tupled)

    override def getSnapshots(initialOrdinal: Long, additionalOrdinals: Seq[Long]): Stream[F, Signed[GlobalSnapshot]] =
      for {
        queue <- Stream.eval(Queue.unbounded[F, (SnapshotOrdinal, Int)])
        _ <- additionalOrdinalsToQueue(queue, additionalOrdinals)
        _ <- Stream(queue).concurrently(downloadAndPushSnapshotOrdinal(queue, initialOrdinal))
        (snapshotOrdinal, nodeNo) <- Stream.fromQueueUnterminated(queue)
        errorOrSnapshot <- downloadSnapshot(snapshotOrdinal, nodeNo)
        snapshot <- getSnapshotOrAddToQueue(errorOrSnapshot, queue)
      } yield snapshot

    def additionalOrdinalsToQueue(queue: Queue[F, (SnapshotOrdinal, Int)], additionalOrdinals: Seq[Long]) =
      Stream.eval(
        additionalOrdinals
          .map(SnapshotOrdinal(_))
          .zipWithIndex
          .map(ordinalWithIndex => (ordinalWithIndex._1, ordinalWithIndex._2 % nodePoolSize))
          .map(ordinalWithNode => queue.offer(ordinalWithNode))
          .sequence
      )

    def downloadAndPushSnapshotOrdinal(queue: Queue[F, (SnapshotOrdinal, Int)], initialOrdinal: Long) =
      Stream(snapshotOrdinalStreamsPool: _*)
        .parJoin(nodePoolSize + 1)
        .scan[Either[Long, (Long, Long)]](Left(initialOrdinal)) {
          case (Left(lastSaved), latestOrdinal) => decideIfNewLastOrdinal(lastSaved, latestOrdinal)
          case (Right(range), latestOrdinal)    => decideIfNewLastOrdinal(range._2, latestOrdinal)
        }
        .flatMap {
          case Left(_)             => Stream.empty
          case Right((start, end)) => Stream.emits(start to end).map(SnapshotOrdinal(_))
        }
        .zip(Stream.emits(0 until nodePoolSize).repeat)
        .evalMap(ordinalWithNodeNo => queue.offer(ordinalWithNodeNo))

    def decideIfNewLastOrdinal(savedOrdinal: Long, latestOrdinal: SnapshotOrdinal): Either[Long, (Long, Long)] =
      if (latestOrdinal.value >= savedOrdinal) Right((savedOrdinal + 1, latestOrdinal.value)) else Left(savedOrdinal)

    def downloadSnapshot(snapshotOrdinal: SnapshotOrdinal, nodeNo: Int) =
      Stream((snapshotOrdinal, nodeNo)).broadcastThrough(snapshotPipesPool: _*)

    def getSnapshotOrAddToQueue(
      errorOrSnapshot: Either[(SnapshotOrdinal, Int), Signed[GlobalSnapshot]],
      queue: Queue[F, (SnapshotOrdinal, Int)]
    ) =
      errorOrSnapshot match {
        case Left((ordinal, nodeNo)) => Stream.eval(queue.offer((ordinal, (nodeNo + 1) % nodePoolSize))) >> Stream.empty
        case Right(value)            => Stream.emit(value)
      }

    def downloadPipe(
      nodeDownload: NodeDownload[F],
      assignedNodeNo: Int
    ): Pipe[F, (SnapshotOrdinal, Int), Either[(SnapshotOrdinal, Int), Signed[GlobalSnapshot]]] = in =>
      in.flatMap { case (ordinal, nodeNo) =>
        if (assignedNodeNo == nodeNo)
          nodeDownload.downloadSnapshot(ordinal).map(_.left.map(ordinal => (ordinal, nodeNo)))
        else Stream.empty
      }

  }

}
