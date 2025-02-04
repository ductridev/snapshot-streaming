package org.constellation.snapshotstreaming

import cats.data.NonEmptyMap

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.util.Try
import org.tessellation.dag.snapshot.SnapshotOrdinal
import org.tessellation.schema.peer.{L0Peer, PeerId}
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import fs2.io.file.Path
import io.circe.parser.decode
import org.http4s.Uri

class Configuration {
  private val config: Config = ConfigFactory.load().resolve()

  private val httpClient = config.getConfig("snapshotStreaming.httpClient")
  private val node = config.getConfig("snapshotStreaming.node")
  private val opensearch = config.getConfig("snapshotStreaming.opensearch")
  private val s3 = config.getConfig("snapshotStreaming.s3")

  val lastSnapshotPath: Path = Path(config.getString("snapshotStreaming.lastSnapshotPath"))
  val l0Peers: NonEmptyMap[PeerId, L0Peer] = NonEmptyMap.fromMapUnsafe(
    SortedMap.from(
      node.getStringList("l0Peers").asScala.toList.map(decode[L0Peer](_).toOption.get).map(p => p.id -> p)
    )
  )
  val pullInterval: FiniteDuration = {
    val d = Duration(node.getString("pullInterval"))
    FiniteDuration(d._1, d._2)
  }
  val pullLimit: PosLong = PosLong.from(node.getLong("pullLimit")).toOption.get
  val initialSnapshot: Option[InitialSnapshot] =
    Try(node.getString("initialSnapshot")).toOption.map(decode[InitialSnapshot](_).toOption.get)
  val terminalSnapshotOrdinal: Option[SnapshotOrdinal] =
    Try(node.getLong("terminalSnapshotOrdinal")).toOption.map(NonNegLong.from(_).toOption.get).map(SnapshotOrdinal(_))

  val httpClientTimeout: Duration = Duration(httpClient.getString("timeout"))
  val httpClientIdleTime: Duration = Duration(httpClient.getString("idleTimeInPool"))

  private val opensearchHost: String = opensearch.getString("host")
  private val opensearchPort: Int = opensearch.getInt("port")
  val opensearchUrl = Uri.unsafeFromString(s"$opensearchHost:$opensearchPort")
  val opensearchTimeout: String = opensearch.getString("timeout")
  val snapshotsIndex: String = opensearch.getString("indexes.snapshots")
  val blocksIndex: String = opensearch.getString("indexes.blocks")
  val transactionsIndex: String = opensearch.getString("indexes.transactions")
  val balancesIndex: String = opensearch.getString("indexes.balances")
  val balancesLimit: Int = opensearch.getInt("balancesLimit")
  val bulkSize: Int = opensearch.getInt("bulkSize")

  val bucketRegion: String = s3.getString("bucketRegion")
  val bucketName: String = s3.getString("bucketName")
  val bucketDir: String = s3.getString("bucketDir")
}
