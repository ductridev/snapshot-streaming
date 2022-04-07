package org.constellation.snapshotstreaming

import java.nio.file.NoSuchFileException

import cats.effect.kernel.Async

import fs2.Stream
import fs2.io.file.{Files, Path}
import fs2.text._
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.syntax._
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait ProcessedSnapshotsService[F[_]] {
  def initialState(): Stream[F, ProcessedSnapshots]
  def saveState(processedSnapshots: ProcessedSnapshots): Stream[F, Unit]
}

object ProcessedSnapshotsService {

  def make[F[_]: Async](configuration: Configuration) = new ProcessedSnapshotsService[F] {

    private val logger = Slf4jLogger.getLoggerFromClass[F](ProcessedSnapshotsService.getClass)

    override def initialState(): Stream[F, ProcessedSnapshots] = Files[F]
      .readAll(Path(configuration.lastOrdinalPath))
      .through(utf8Decode)
      .flatMap(str => Stream.eval(Async[F].delay(decode[ProcessedSnapshots](str))).rethrow)
      .head
      .handleErrorWith {
        case ex: NoSuchFileException =>
          Stream.eval(
            logger.warn(
              s"Couldn't find file with initial ordinal. Initializing with default values. ${ex.getLocalizedMessage}."
            )
          ) >> Stream.emit(ProcessedSnapshots(-1, Nil))
        case ex =>
          Stream.eval(logger.error(ex)("Couldn't load data from file with initial ordinal.")) >> Stream.raiseError(ex)
      }
      .evalTap(init => logger.info(s"Loaded file with initial ordinal: $init."))

    override def saveState(processedSnapshots: ProcessedSnapshots): Stream[F, Unit] =
      Stream
        .emit(processedSnapshots.asJson.spaces2)
        .through(utf8Encode)
        .through(Files[F].writeAll(Path(configuration.lastOrdinalPath)))

  }

}

case class ProcessedSnapshots(lastProcessedOrdinal: Long, gaps: List[Long])
