package org.constellation.snapshotstreaming

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

import scala.collection.JavaConverters._

class Configuration {

  private val config: Config = ConfigFactory.load().resolve()
  private val elasticsearch =
    config.getConfig("snapshot-streaming.elasticsearch")
  private val bucket = config.getConfig("snapshot-streaming.bucket")
  private val interval = config.getConfig("snapshot-streaming.interval")

  val startingHeight: Long =
    Try(interval.getLong("startingHeight")).getOrElse(2L)
  val endingHeight
    : Option[Long] = Try(interval.getLong("endingHeight")).toOption

  val elasticsearchUrl: String =
    elasticsearch.getString("url")

  val elasticsearchPort: Int =
    elasticsearch.getInt("port")

  val elasticsearchTransactionsIndex: String =
    elasticsearch.getString("indexes.transactions")

  val elasticsearchCheckpointBlocksIndex: String =
    elasticsearch.getString("indexes.checkpoint-blocks")

  val elasticsearchSnapshotsIndex: String =
    elasticsearch.getString("indexes.snapshots")

  val elasticsearchBalancesIndex: String =
    elasticsearch.getString("indexes.balances")

  val bucketRegion: String =
    bucket.getString("region")

  val bucketNames: List[String] =
    bucket.getStringList("urls").asScala.toList
}
