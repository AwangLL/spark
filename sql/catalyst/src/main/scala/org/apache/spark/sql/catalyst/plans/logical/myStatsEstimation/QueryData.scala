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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, Project}

class QueryData {
  // (alias -> table)
  private val tables: mutable.Map[String, String] = mutable.Map()
  private val joins: mutable.Set[Expression] = mutable.Set()
  private val predicates = ExpressionTree("AND")

  def init(plan: Project): Unit = {
    plan.child match {
      case child: Filter =>
        merge(child.queryData)
      case child: Join =>
        merge(child.queryData)
      case _ =>
        throw new RuntimeException("Unsupported plan in Project: " + plan.child.nodeName)
    }
  }

  def init(plan: Filter): Unit = {
    val table = getTableNameFrom(plan)
    val alias = getTableAliasFrom(plan)
    tables += (alias -> table)
    extract_predicate_condition(predicates, plan.condition)
  }

  def init(plan: Join): Unit = {
    val leftQuery = plan.left.queryData
    val rightQuery = plan.right.queryData
    merge(leftQuery)
    merge(rightQuery)
    joins ++= extract_join_condition(plan.condition.get)
  }

  def ++(other: QueryData): QueryData = {
    val mergedQueryData = new QueryData
    mergedQueryData.merge(this)
    mergedQueryData.merge(other)
    mergedQueryData
  }

  def merge(other: QueryData): Unit = {
    tables ++= other.tables
    joins ++= other.joins
    predicates.extend(other.predicates)
  }

  override def toString: String = {
    var sqlStr = "SELECT COUNT(*) FROM "
    var isFirst = true
    for ((alias, table) <- tables) {
      if (isFirst) {
        sqlStr += s"$table AS $alias"
        isFirst = false
      } else {
        sqlStr += s", $table AS $alias"
      }
    }

    val predicateStr = predicates.toString
    if (predicateStr != "") sqlStr += s" WHERE $predicateStr"

    if (predicateStr == "" && joins.nonEmpty) {
      sqlStr += " WHERE"
      isFirst = true
    }
    joins.foreach { join =>
      if (isFirst) {
        sqlStr += s" ${join.sql.stripPrefix("(").stripSuffix(")")}"
        isFirst = false
      } else {
        sqlStr += s" AND ${join.sql.stripPrefix("(").stripSuffix(")")}"
      }
    }

    sqlStr + ';'
  }

  private def getTableNameFrom(plan: Filter): String = {
    val str = plan.child.toString
    str.substring(31, str.indexOf('['))
  }

  private def getTableAliasFrom(plan: Filter): String = {
    val str = plan.condition.sql
    val pos = str.indexOf('.')
    var alias = ""
    var flag = true
    for (i <- pos - 1 to 0 by -1 if flag) {
      val ch = str(i)
      if (ch != ' ' && ch != '(') {
        alias += ch
      } else flag = false
    }

    alias.reverse
  }

  private def extract_predicate_condition(tree: ExpressionTree,
                                          condition: Expression): Unit = {
    condition match {
      case condition: And =>
        if (tree.tag == "AND") {
          extract_predicate_condition(tree, condition.left)
          extract_predicate_condition(tree, condition.right)
        } else {
          val new_tree = ExpressionTree("AND")
          extract_predicate_condition(new_tree, condition.left)
          extract_predicate_condition(new_tree, condition.right)
          tree.addChild(new_tree)
        }
      case condition: Or =>
        if (tree.tag == "OR") {
          extract_predicate_condition(tree, condition.left)
          extract_predicate_condition(tree, condition.right)
        } else {
          val new_tree = ExpressionTree("OR")
          extract_predicate_condition(new_tree, condition.left)
          extract_predicate_condition(new_tree, condition.right)
          tree.addChild(new_tree)
        }
      case _ =>
        tree.addChild(ExpressionTree("", condition))
    }
  }

  private def extract_join_condition(condition: Expression): mutable.Set[Expression] = {
    condition match {
      case expression: And =>
        extract_join_condition(expression.left) ++ extract_join_condition(expression.right)
      case expression: EqualTo =>
        mutable.Set(expression)
      case _ =>
        throw new RuntimeException("Unsupported expression in join: " + condition.toString)
    }
  }
}