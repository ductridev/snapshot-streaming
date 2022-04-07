package org.constellation.snapshotstreaming.opensearch.schema

import java.util.Date

import io.circe.Encoder
import io.circe.generic.semiauto._

final case class Snapshot(
  hash: String,
  ordinal: Long,
  height: Long,
  subHeight: Long,
  lastSnapshotHash: String,
  blocks: Set[String],
  rewards: Set[RewardTransaction],
  timestamp: Date
)

object Snapshot {

  implicit val dateEncoder: Encoder[Date] =
    Encoder.encodeString.contramap(date => date.toInstant.toString)

  implicit val snapshotEncoder: Encoder[Snapshot] = deriveEncoder

}
