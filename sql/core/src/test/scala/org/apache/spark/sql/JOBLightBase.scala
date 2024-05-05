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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

trait JOBLightBase extends SharedSparkSession with JOBLightSchema {

  // job-light from https://github.com/andreaskipf/learnedcardinalities
  val jobLightAllQueries: Seq[String] = Seq(
      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
      "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29", "q30",
      "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
      "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
      "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
      "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70")

  protected def injectStats: Boolean = false

  override protected def sparkConf: SparkConf = {
    if (injectStats) {
      super.sparkConf
        .set(SQLConf.MAX_TO_STRING_FIELDS, Int.MaxValue)
        .set(SQLConf.CBO_ENABLED, true)
        .set(SQLConf.PLAN_STATS_ENABLED, true)
        .set(SQLConf.JOIN_REORDER_ENABLED, true)
    } else {
      super.sparkConf.set(SQLConf.MAX_TO_STRING_FIELDS, Int.MaxValue)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTables()
  }

  override def afterAll(): Unit = {
    dropTables()
    super.afterAll()
  }

  protected def partitionedByClause(tableName: String): String = {
    tablePartitionColumns.get(tableName) match {
      case Some(cols) if cols.nonEmpty => s"PARTITIONED BY (${cols.mkString(", ")})"
      case _ => ""
    }
  }

  val tableNames: Iterable[String] = tableColumns.keys

  def createTable(
      spark: SparkSession,
      tableName: String,
      format: String = "csv",
      options: Seq[String] = Nil): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE `$tableName` (${tableColumns(tableName)})
         |USING $format
         |${partitionedByClause(tableName)}
         |${options.mkString("\n")}
       """.stripMargin)
  }

  def createTables(): Unit = {
    tableNames.foreach { tableName =>
      createTable(spark, tableName)
      if (injectStats) {
        // To simulate plan generation on actual TPC-DS data, injects data stats here
        spark.sessionState.catalog.alterTableStats(
          TableIdentifier(tableName), Some(TPCDSTableStats.sf100TableStats(tableName)))
      }
    }
  }

  def dropTables(): Unit = {
    tableNames.foreach { tableName =>
      spark.sessionState.catalog.dropTable(TableIdentifier(tableName), true, true)
    }
  }
}
