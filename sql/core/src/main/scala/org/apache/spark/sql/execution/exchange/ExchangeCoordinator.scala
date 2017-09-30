/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.exchange

import java.util.{HashMap => JHashMap, Map => JMap}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{MapOutputStatistics, ShuffleDependency, SimpleFutureAction}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{ShuffledRowRDD, SparkPlan}

/**
 * A coordinator used to determines how we shuffle data between stages generated by Spark SQL.
 * Right now, the work of this coordinator is to determine the number of post-shuffle partitions
 * for a stage that needs to fetch shuffle data from one or multiple stages.
 *
 * A coordinator is constructed with three parameters, `numExchanges`,
 * `targetPostShuffleInputSize`, and `minNumPostShufflePartitions`.
 *  - `numExchanges` is used to indicated that how many [[ShuffleExchange]]s that will be registered
 *    to this coordinator. So, when we start to do any actual work, we have a way to make sure that
 *    we have got expected number of [[ShuffleExchange]]s.
 *  - `targetPostShuffleInputSize` is the targeted size of a post-shuffle partition's
 *    input data size. With this parameter, we can estimate the number of post-shuffle partitions.
 *    This parameter is configured through
 *    `spark.sql.adaptive.shuffle.targetPostShuffleInputSize`.
 *  - `targetPostShuffleRowCount` is the targeted row count of a post-shuffle partition's
 *    input row count. This is set through
 *    `spark.sql.adaptive.shuffle.adaptiveTargetPostShuffleRowCount`.
 *  - `minNumPostShufflePartitions` is an optional parameter. If it is defined, this coordinator
 *    will try to make sure that there are at least `minNumPostShufflePartitions` post-shuffle
 *    partitions.
 *
 * The workflow of this coordinator is described as follows:
 *  - Before the execution of a [[SparkPlan]], for a [[ShuffleExchange]] operator,
 *    if an [[ExchangeCoordinator]] is assigned to it, it registers itself to this coordinator.
 *    This happens in the `doPrepare` method.
 *  - Once we start to execute a physical plan, a [[ShuffleExchange]] registered to this
 *    coordinator will call `postShuffleRDD` to get its corresponding post-shuffle
 *    [[ShuffledRowRDD]].
 *    If this coordinator has made the decision on how to shuffle data, this [[ShuffleExchange]]
 *    will immediately get its corresponding post-shuffle [[ShuffledRowRDD]].
 *  - If this coordinator has not made the decision on how to shuffle data, it will ask those
 *    registered [[ShuffleExchange]]s to submit their pre-shuffle stages. Then, based on the
 *    size statistics of pre-shuffle partitions, this coordinator will determine the number of
 *    post-shuffle partitions and pack multiple pre-shuffle partitions with continuous indices
 *    to a single post-shuffle partition whenever necessary.
 *  - Finally, this coordinator will create post-shuffle [[ShuffledRowRDD]]s for all registered
 *    [[ShuffleExchange]]s. So, when a [[ShuffleExchange]] calls `postShuffleRDD`, this coordinator
 *    can lookup the corresponding [[RDD]].
 *
 * The strategy used to determine the number of post-shuffle partitions is described as follows.
 * To determine the number of post-shuffle partitions, we have a target input size for a
 * post-shuffle partition. Once we have size statistics of pre-shuffle partitions from stages
 * corresponding to the registered [[ShuffleExchange]]s, we will do a pass of those statistics and
 * pack pre-shuffle partitions with continuous indices to a single post-shuffle partition until
 * adding another pre-shuffle partition would cause the size of a post-shuffle partition to be
 * greater than the target size.
 *
 * For example, we have two stages with the following pre-shuffle partition size statistics:
 * stage 1: [100 MB, 20 MB, 100 MB, 10MB, 30 MB]
 * stage 2: [10 MB,  10 MB, 70 MB,  5 MB, 5 MB]
 * assuming the target input size is 128 MB, we will have three post-shuffle partitions,
 * which are:
 *  - post-shuffle partition 0: pre-shuffle partition 0 (size 110 MB)
 *  - post-shuffle partition 1: pre-shuffle partition 1 (size 30 MB)
 *  - post-shuffle partition 2: pre-shuffle partition 2 (size 170 MB)
 *  - post-shuffle partition 3: pre-shuffle partition 3 and 4 (size 50 MB)
 */
class ExchangeCoordinator(
    advisoryTargetPostShuffleInputSize: Long,
    targetPostShuffleRowCount: Long,
    minNumPostShufflePartitions: Option[Int] = None)
  extends Logging {

  /**
   * Estimates partition start indices for post-shuffle partitions based on
   * mapOutputStatistics provided by all pre-shuffle stages.
   */
  def estimatePartitionStartIndices(
      mapOutputStatistics: Array[MapOutputStatistics]): Array[Int] = {
    estimatePartitionStartEndIndices(mapOutputStatistics, mutable.HashSet.empty)._1
  }

  /**
   * Estimates partition start indices for post-shuffle partitions based on
   * mapOutputStatistics provided by all pre-shuffle stages and omitted skewed partitions which have
   * been taken care of in HandleSkewedJoin.
   */
  def estimatePartitionStartEndIndices(
      mapOutputStatistics: Array[MapOutputStatistics],
      omittedPartitions: mutable.HashSet[Int]): (Array[Int], Array[Int]) = {

    assert(omittedPartitions.size < mapOutputStatistics(0).bytesByPartitionId.length,
      "All partitions are skewed.")

    // If minNumPostShufflePartitions is defined, it is possible that we need to use a
    // value less than advisoryTargetPostShuffleInputSize as the target input size of
    // a post shuffle task.
    val targetPostShuffleInputSize = minNumPostShufflePartitions match {
      case Some(numPartitions) =>
        val totalPostShuffleInputSize = mapOutputStatistics.map(_.bytesByPartitionId.sum).sum
        // The max at here is to make sure that when we have an empty table, we
        // only have a single post-shuffle partition.
        // There is no particular reason that we pick 16. We just need a number to
        // prevent maxPostShuffleInputSize from being set to 0.
        val maxPostShuffleInputSize =
          math.max(math.ceil(totalPostShuffleInputSize / numPartitions.toDouble).toLong, 16)
        math.min(maxPostShuffleInputSize, advisoryTargetPostShuffleInputSize)

      case None => advisoryTargetPostShuffleInputSize
    }

    logInfo(
      s"advisoryTargetPostShuffleInputSize: $advisoryTargetPostShuffleInputSize, " +
      s"targetPostShuffleInputSize $targetPostShuffleInputSize. ")

    // Make sure we do get the same number of pre-shuffle partitions for those stages.
    val distinctNumPreShufflePartitions =
      mapOutputStatistics.map(stats => stats.bytesByPartitionId.length).distinct
    // The reason that we are expecting a single value of the number of pre-shuffle partitions
    // is that when we add Exchanges, we set the number of pre-shuffle partitions
    // (i.e. map output partitions) using a static setting, which is the value of
    // spark.sql.shuffle.partitions. Even if two input RDDs are having different
    // number of partitions, they will have the same number of pre-shuffle partitions
    // (i.e. map output partitions).
    assert(
      distinctNumPreShufflePartitions.length == 1,
      "There should be only one distinct value of the number pre-shuffle partitions " +
        "among registered Exchange operator.")
    val numPreShufflePartitions = distinctNumPreShufflePartitions.head

    val partitionStartIndices = ArrayBuffer[Int]()
    val partitionEndIndices = ArrayBuffer[Int]()

    def nextStartIndex(i: Int): Int = {
      var index = i
      while (index < numPreShufflePartitions && omittedPartitions.contains(index)) {
        index = index + 1
      }
      index
    }

    def partitionSizeAndRowCount(partitionId: Int): (Long, Long) = {
      var size = 0L
      var rowCount = 0L
      var j = 0
      while (j < mapOutputStatistics.length) {
        size += mapOutputStatistics(j).bytesByPartitionId(partitionId)
        rowCount += mapOutputStatistics(j).rowsByPartitionId(partitionId)
        j += 1
      }
      (size, rowCount)
    }

    val firstStartIndex = nextStartIndex(0)
    partitionStartIndices += firstStartIndex
    var postShuffleInput = partitionSizeAndRowCount(firstStartIndex)

    var i = firstStartIndex
    var nextIndex = nextStartIndex(i + 1)
    while (nextIndex < numPreShufflePartitions) {
      val nextShuffleInput = partitionSizeAndRowCount(nextIndex)
      // If the next partition is omitted, or including the nextShuffleInputSize would exceed the
      // target partition size, then start a new partition.
      if (nextIndex != i + 1 ||
        postShuffleInput._1 +  nextShuffleInput._1 > targetPostShuffleInputSize ||
        postShuffleInput._2 + nextShuffleInput._2 > targetPostShuffleRowCount) {
        partitionEndIndices += i + 1
        partitionStartIndices += nextIndex
        postShuffleInput = nextShuffleInput
        i = nextIndex
      } else {
        postShuffleInput = (postShuffleInput._1 + nextShuffleInput._1,
          postShuffleInput._2 + nextShuffleInput._2)

        i += 1
      }
      nextIndex = nextStartIndex(nextIndex + 1)
    }
    partitionEndIndices += i + 1

    (partitionStartIndices.toArray, partitionEndIndices.toArray)
  }

  override def toString: String = {
    s"coordinator[target post-shuffle partition size: $advisoryTargetPostShuffleInputSize]" +
      s"coordinator[target post-shuffle row count: $targetPostShuffleRowCount]"
  }
}
