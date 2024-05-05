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
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.SizeInBytesOnlyStatsPlanVisitor

/**
 * A [[LogicalPlanVisitor]] that computes the statistics for the cost-based optimizer.
 */
object BasicStatsPlanVisitor {

  def visit(p: LogicalPlan): Statistics = p match {
    case p: Join => visitJoin(p)
    case p: Filter => visitFilter(p)
    case p: Project => visitProject(p)
    case p: LeafNode => visitLeafNode(p)
    case p: LogicalPlan => default(p)
  }

  private def default(p: LogicalPlan): Statistics = {
    throw new RuntimeException("Unsupported plan type: " + p.nodeName)
  }

  private def visitLeafNode(p: LeafNode): Statistics = {
    p.nodeName match {
      case "LogicalRelation" =>
        p.computeStats()
      case _ =>
        throw new RuntimeException("Unsupported LeafNode type: " + p.nodeName)
    }
  }

  private def visitFilter(p: Filter): Statistics = {
    Estimator.estimate(p)
  }

  private def visitJoin(p: Join): Statistics = {
    Estimator.estimate(p)
  }

  private def visitProject(p: Project): Statistics = {
    Estimator.estimate(p)
  }

  /** Falls back to the estimation computed by [[SizeInBytesOnlyStatsPlanVisitor]]. */
  private def fallback(p: LogicalPlan): Statistics = SizeInBytesOnlyStatsPlanVisitor.visit(p)
}
