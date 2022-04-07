package org.constellation.snapshotstreaming.opensearch.schema

import java.util.Date

import io.circe.Encoder
import io.circe.generic.semiauto._

final case class Block(
  hash: String,
  height: Long,
  parent: Set[BlockReference],
  transactions: Set[String],
  snapshotHash: String,
  snapshotOrdinal: Long,
  timestamp: Date
)

object Block {

  implicit val dateEncoder: Encoder[Date] =
    Encoder.encodeString.contramap(date => date.toInstant.toString)

  implicit val blockEncoder: Encoder[Block] = deriveEncoder
}

final case class BlockReference(
  hash: String,
  height: Long
)

object BlockReference {
  implicit val blockReferenceEncoder: Encoder[BlockReference] = deriveEncoder
}
