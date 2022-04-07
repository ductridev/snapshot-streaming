package org.constellation.snapshotstreaming.opensearch.schema

import java.util.Date

import io.circe.Encoder
import io.circe.generic.semiauto._

final case class Balances(
  snapshotHash: String,
  snapshotOrdinal: Long,
  balances: Seq[AddressBalance],
  docPart: Long,
  docTotalParts: Long,
  timestamp: Date
)

object Balances {

  implicit val dateEncoder: Encoder[Date] =
    Encoder.encodeString.contramap(date => date.toInstant.toString)

  implicit val balancesEncoder: Encoder[Balances] = deriveEncoder

  implicit class BalancesOps(balances: Balances) {
    def docId = s"${balances.snapshotOrdinal}docPart${balances.docPart}"
  }

}
