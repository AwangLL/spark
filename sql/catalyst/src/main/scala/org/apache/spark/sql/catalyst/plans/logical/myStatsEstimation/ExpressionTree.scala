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

import org.apache.spark.sql.catalyst.expressions.{Contains, EndsWith, EqualTo, Expression, IsNotNull, Like, Not, StartsWith}

class ExpressionTree(var tag: String,
                     var value: Expression,
                     var children: List[ExpressionTree] = Nil) {
  def extend(other: ExpressionTree): Unit = {
    other.children.foreach(child => addChild(child))
  }

  def addChild(child: ExpressionTree): Unit = {
    children = children :+ child
  }

  override def toString: String = traversal(this)

  private def traversal(tree: ExpressionTree): String = {
    var str = ""
    val op = tree.tag
    var isFirst = true
    tree.children.foreach { node =>
      var nodeStr = ""
      if (node.tag == "") {
        node.value match {
          case expression: IsNotNull => nodeStr = ""
          case expression: Not =>
            expression.child match {
              case child: EqualTo =>
                nodeStr = s"${child.left.sql} != ${child.right.sql}"
              case child: Like =>
                nodeStr = s"${child.left.sql} NOT LIKE '${child.right}'"
              case child: Contains =>
                nodeStr = s"${child.left.sql} NOT LIKE '%${child.right}%'"
              case child: StartsWith =>
                nodeStr = s"${child.left.sql} NOT LIKE '${child.right}%'"
              case child: EndsWith =>
                nodeStr = s"${child.left.sql} NOT LIKE '%${child.right}'"
              case _ =>
                nodeStr = node.value.sql.stripPrefix("(").stripSuffix(")")
            }
          case expression: Like =>
            nodeStr = s"${expression.left.sql} LIKE '${expression.right}'"
          case expression: Contains =>
            nodeStr = s"${expression.left.sql} LIKE '%${expression.right}%'"
          case expression: StartsWith =>
            nodeStr = s"${expression.left.sql} LIKE '${expression.right}%'"
          case expression: EndsWith =>
            nodeStr = s"${expression.left.sql} LIKE '%${expression.right}'"
          case _ =>
            nodeStr = node.value.sql.stripPrefix("(").stripSuffix(")")
        }
      } else {
        nodeStr = s"(${traversal(node)})"
      }

      if (nodeStr != "") {
        if (isFirst) {
          str += nodeStr
          isFirst = false
        } else {
          str += s" $op $nodeStr"
        }
      }
    }
    str
  }
}

object ExpressionTree {
  def apply(tag: String, value: Expression = null): ExpressionTree = new ExpressionTree(tag, value)
}