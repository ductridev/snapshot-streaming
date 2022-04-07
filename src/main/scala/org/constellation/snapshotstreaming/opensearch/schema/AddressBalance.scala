package org.constellation.snapshotstreaming.opensearch.schema

import java.util.Date

import io.circe.Encoder
import io.circe.generic.semiauto._

final case class AddressBalance(
  address: String,
  balance: Long,
  snapshotHash: String,
  snapshotOrdinal: Long,
  timestamp: Date
)

object AddressBalance {

  implicit val dateEncoder: Encoder[Date] =
    Encoder.encodeString.contramap(date => date.toInstant.toString)

  implicit val snapshotEncoder: Encoder[AddressBalance] = deriveEncoder

  implicit class AddressBalanceOps(balance: AddressBalance) {
    def docId = s"${balance.address}${balance.snapshotOrdinal}"
  }

}
