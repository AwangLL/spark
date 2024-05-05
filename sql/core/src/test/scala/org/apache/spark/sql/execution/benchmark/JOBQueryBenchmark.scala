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

package org.apache.spark.sql.execution.benchmark

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{JOBSchema, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

object JOBQueryBenchmark extends SqlBasedBenchmark with Logging {
  val tables: Seq[String] = Seq(
    "aka_name", "aka_title", "cast_info", "char_name", "comp_cast_type",
    "company_name", "company_type", "complete_cast", "info_type", "keyword",
    "kind_type", "link_type", "movie_companies", "movie_info_idx", "movie_keyword",
    "movie_link", "name", "role_type", "title", "movie_info", "person_info")

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setMaster(System.getProperty("spark.sql.test.master", "local[1]"))
      .setAppName("test-sql-context")
      .set("spark.sql.parquet.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", System.getProperty("spark.sql.shuffle.partitions", "4"))
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "3g")
      .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")

    SparkSession.builder.config(conf).getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmarkArgs = new JOBQueryBenchmarkArguments(mainArgs)

    // join-order-benchmark from https://github.com/andreaskipf/learnedcardinalities
    val jobQueries = Seq(
      "1a", "1b", "1c", "1d",
      "2a", "2b", "2c", "2d",
      "3a", "3b", "3c",
      "4a", "4b", "4c",
      "5a", "5b", "5c",
      "6a", "6b", "6c", "6d", "6e", "6f",
      "7a", "7b", "7c",
      "8a", "8b", "8c", "8d",
      "9a", "9b", "9c", "9d",
      "10a", "10b", "10c",
      "11a", "11b", "11c", "11d",
      "12a", "12b", "12c",
      "13a", "13b", "13c", "13d",
      "14a", "14b", "14c",
      "15a", "15b", "15c", "15d",
      "16a", "16b", "16c", "16d",
      "17a", "17b", "17c", "17d", "17e", "17f",
      "18a", "18b", "18c",
      "19a", "19b", "19c", "19d",
      "20a", "20b", "20c",
      "21a", "21b", "21c",
      "22a", "22b", "22c", "22d",
      "23a", "23b", "23c",
      "24a", "24b",
      "25a", "25b", "25c",
      "26a", "26b", "26c",
      "27a", "27b", "27c",
      "28a", "28b", "28c",
      "29a", "29b", "29c",
      "30a", "30b", "30c",
      "31a", "31b", "31c",
      "32a", "32b",
      "33a", "33b", "33c")

    val excludedQuery = Set("7a", "7c")

    val queriesToRun = filterQueries(jobQueries.filterNot(excludedQuery.contains),
      benchmarkArgs.queryFilter)

    if (queriesToRun.isEmpty) {
      throw new RuntimeException("Empty queries to run.")
    }

    val tableSizes = setupTables(benchmarkArgs.dataLocation,
      JOBSchemaHelper$.getTableColumns)

    if (benchmarkArgs.cboEnabled) {
      spark.sql(s"SET ${SQLConf.CBO_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.PLAN_STATS_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.JOIN_REORDER_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.HISTOGRAM_ENABLED.key}=true")

      // Analyze all the tables before running job-light queries
      val startTime = System.nanoTime()
      tables.foreach { tableName =>
        spark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR ALL COLUMNS")
      }
      logInfo("The elapsed time to analyze all the tables is " +
        s"${(System.nanoTime() - startTime) / NANOS_PER_SECOND.toDouble} seconds")
    } else if (benchmarkArgs.mycboEnabled) {
      spark.sql(s"SET ${SQLConf.CBO_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.PLAN_STATS_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.JOIN_REORDER_ENABLED.key}=true")
      spark.sql(s"SET spark.sql.mycbo.enabled=true")
      spark.sql(s"SET ${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key}=false")
      // spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")

      // Analyze all the tables before running job-light queries
      val startTime = System.nanoTime()
      tables.foreach { tableName =>
        spark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
      }
      logInfo("The elapsed time to analyze all the tables is " +
        s"${(System.nanoTime() - startTime) / NANOS_PER_SECOND.toDouble} seconds")
    } else {
      spark.sql(s"SET ${SQLConf.CBO_ENABLED.key}=false")
    }

    runJobQueries(queryLocation = "job", queries = queriesToRun, tableSizes)
  }

  def setupTables(dataLocation: String, tableColumns: Map[String, StructType]): Map[String, Long] =
    tables.map { tableName =>
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      val options = Map("path" -> s"$dataLocation/$tableName.csv")
      spark.catalog.createTable(tableName, "csv", tableColumns(tableName), options)
      Try {
        spark.sql(s"ALTER TABLE $tableName RECOVER PARTITIONS")
      }.getOrElse {
        logInfo(s"Recovering partitions of table $tableName failed")
      }
      tableName -> spark.table(tableName).count()
    }.toMap

  def runJobQueries(queryLocation: String,
                    queries: Seq[String],
                    tableSizes: Map[String, Long],
                    nameSuffix: String = ""): Unit = {
    queries.foreach { name =>
      val queryString = resourceToString(s"$queryLocation/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)

      val queryRelations = scala.collection.mutable.HashSet[String]()
      spark.sql(queryString).queryExecution.analyzed.foreach {
        case SubqueryAlias(alias, _: LogicalRelation) =>
          queryRelations.add(alias.name)
        case LogicalRelation(_, _, Some(catalogTable), _) =>
          queryRelations.add(catalogTable.identifier.table)
        case HiveTableRelation(tableMeta, _, _, _, _) =>
          queryRelations.add(tableMeta.identifier.table)
        case _ =>
      }
      val numRows = queryRelations.map(tableSizes.getOrElse(_, 0L)).sum
      val benchmark = new Benchmark("Join Order Benchmark", numRows, 2, output = output)
      benchmark.addCase(s"$name$nameSuffix") { _ =>
        spark.sql(queryString).noop()
      }
      benchmark.run()
    }
  }

  private def filterQueries(origQueries: Seq[String],
                            queryFilter: Set[String],
                            nameSuffix: String = ""): Seq[String] = {
    if (queryFilter.nonEmpty) {
      if (nameSuffix.nonEmpty) {
        origQueries.filter { name => queryFilter.contains(s"$name$nameSuffix") }
      } else {
        origQueries.filter(queryFilter.contains)
      }
    } else {
      origQueries
    }
  }
}

object JOBSchemaHelper$ extends JOBSchema {
  def getTableColumns: Map[String, StructType] =
    tableColumns.map(kv => kv._1 -> StructType.fromDDL(kv._2))
}