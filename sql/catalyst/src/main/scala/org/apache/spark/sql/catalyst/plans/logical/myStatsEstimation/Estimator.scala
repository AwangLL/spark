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

package org.apache.spark.sql.catalyst.plans.logical.myStatsEstimation

import org.apache.spark.sql.catalyst.plans.logical._

object Estimator {
  private def getCardinality(query: QueryData): BigInt = {
    val cardinality = Communicator.socket(query.toString.getBytes())
    cardinality.toInt
  }

  def estimate(plan: Filter): Statistics = {
    val childStats = plan.child.stats
    val cardinality = Estimator.getCardinality(plan.queryData)
    val filteredSize = childStats.sizeInBytes * cardinality / childStats.rowCount.get

    childStats.copy(sizeInBytes = filteredSize, rowCount = Some(cardinality))
  }

  def estimate(plan: Project): Statistics = {
    val child = plan.child
    val childStats = child.stats

    childStats.copy(sizeInBytes = childStats.sizeInBytes, rowCount = childStats.rowCount)
  }

  def estimate(plan: Join): Statistics = {
    val leftStats = plan.left.stats
    val rightStats = plan.right.stats

    val cardinality = Estimator.getCardinality(plan.queryData)
    val leftSizePerRow = leftStats.sizeInBytes / leftStats.rowCount.get
    val rightSizePerRow = rightStats.sizeInBytes / rightStats.rowCount.get

    Statistics(
      sizeInBytes = (leftSizePerRow + rightSizePerRow) * cardinality,
      rowCount = Some(cardinality))
  }
}
