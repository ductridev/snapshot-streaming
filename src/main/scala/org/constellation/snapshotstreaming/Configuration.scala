package org.constellation.snapshotstreaming

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.Try

import com.typesafe.config.{Config, ConfigFactory}
import org.http4s.Uri

class Configuration {
  private val config: Config = ConfigFactory.load().resolve()
  private val httpclient = config.getConfig("snapshot-streaming.httpclient")
  private val node = config.getConfig("snapshot-streaming.node")

  private val opensearch =
    config.getConfig("snapshot-streaming.opensearch")

  val lastOrdinalPath: String =
    Try(config.getString("snapshot-streaming.last-ordinal-path")).getOrElse("last-ordinal-path")

  val httpClientTimeout = Duration(httpclient.getString("timeout"))
  val httpClientIdleTime = Duration(httpclient.getString("idleTimeInPool"))

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

  val nodeIntervalInSeconds: Int = node.getInt("retryIntervalInSeconds")
  val nodeUrls: List[String] = node.getStringList("urls").asScala.toList

}
