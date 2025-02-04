package org.constellation.snapshotstreaming.opensearch

import java.util.Date

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.kryo.KryoSerializer

import com.sksamuel.elastic4s.ElasticApi.updateById
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.requests.update.UpdateRequest
import org.constellation.snapshotstreaming.Configuration
import org.constellation.snapshotstreaming.opensearch.mapper.GlobalSnapshotMapper
import org.constellation.snapshotstreaming.opensearch.schema._
import org.tessellation.security.Hashed

trait UpdateRequestBuilder[F[_]] {
  def bulkUpdateRequests(globalSnapshot: Hashed[GlobalSnapshot], timestamp: Date): F[Seq[Seq[UpdateRequest]]]
}

object UpdateRequestBuilder {

  def make[F[_]: Async: KryoSerializer](config: Configuration): UpdateRequestBuilder[F] =
    make(GlobalSnapshotMapper.make(), config)

  def make[F[_]: Async](mapper: GlobalSnapshotMapper[F], config: Configuration): UpdateRequestBuilder[F] =
    new UpdateRequestBuilder[F] {

      def bulkUpdateRequests(globalSnapshot: Hashed[GlobalSnapshot], timestamp: Date): F[Seq[Seq[UpdateRequest]]] =
        for {
          snapshot <- mapper.mapSnapshot(globalSnapshot, timestamp)
          blocks <- mapper.mapBlocks(globalSnapshot, timestamp)
          transactions <- mapper.mapTransactions(globalSnapshot, timestamp)
          balances = mapper.mapBalances(globalSnapshot, timestamp)
        } yield updateRequests(snapshot, blocks, transactions, balances).grouped(config.bulkSize).toSeq

      def updateRequests[T](
        snapshot: Snapshot,
        blocks: Seq[Block],
        transactions: Seq[Transaction],
        balances: Seq[AddressBalance]
      ): Seq[UpdateRequest] = Seq(updateById(config.snapshotsIndex, snapshot.hash).docAsUpsert(snapshot)) ++
        blocks.map(block => updateById(config.blocksIndex, block.hash).docAsUpsert(block)) ++
        transactions.map(transaction =>
          updateById(config.transactionsIndex, transaction.hash).docAsUpsert(transaction)
        ) ++
        balances.map(balance => updateById(config.balancesIndex, balance.docId).docAsUpsert(balance))

    }

}
