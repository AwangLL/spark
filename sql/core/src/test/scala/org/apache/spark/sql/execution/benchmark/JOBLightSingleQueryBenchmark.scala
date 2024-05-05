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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

object JOBLightSingleQueryBenchmark extends SqlBasedBenchmark with Logging {
  val tables: Seq[String] = Seq("cast_info", "movie_companies", "movie_info_idx", "movie_keyword",
    "title", "movie_info")

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
    val benchmarkArgs = new JOBSingleQueryBenchmarkArguments(mainArgs)

    val queriesToRun = benchmarkArgs.query

    if (queriesToRun.isEmpty) {
      throw new RuntimeException("Empty queries to run.")
    }

    val tableSizes = setupTables(benchmarkArgs.dataLocation,
      JOBLightSchemaHelper$.getTableColumns)

    // scalastyle:off println
    if (benchmarkArgs.cboEnabled) {
      println("=====with cbo=====")
      spark.sql(s"SET ${SQLConf.CBO_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.PLAN_STATS_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.JOIN_REORDER_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.HISTOGRAM_ENABLED.key}=true")

      // Analyze all the tables before running job-light queries
      val startTime = System.nanoTime()
      tables.foreach { tableName =>
        println(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR ALL COLUMNS")
        spark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR ALL COLUMNS")
      }
      println("The elapsed time to analyze all the tables is " +
        s"${(System.nanoTime() - startTime) / NANOS_PER_SECOND.toDouble} seconds")

      runJobLightQueries(queryLocation = "job-light", queries = queriesToRun, tableSizes)
    }

    if (benchmarkArgs.mycboEnabled) {
      println("====with mycbo====")
      spark.sql(s"SET ${SQLConf.CBO_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.PLAN_STATS_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.JOIN_REORDER_ENABLED.key}=true")
      spark.sql("SET spark.sql.mycbo.enabled=true")
      // spark.sql(s"SET ${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key}=false")
      // spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")

      // Analyze all the tables before running job-light queries
      if (!benchmarkArgs.cboEnabled) {
        val startTime = System.nanoTime()
        tables.foreach { tableName =>
          spark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
        }
        println("The elapsed time to analyze all the tables is " +
          s"${(System.nanoTime() - startTime) / NANOS_PER_SECOND.toDouble} seconds")
      }

      runJobLightQueries(queryLocation = "job-light", queries = queriesToRun, tableSizes)
    }

    if (benchmarkArgs.rboEnabled) {
      println("=====with rbo=====")
      spark.sql(s"SET ${SQLConf.CBO_ENABLED.key}=false")
      spark.sql(s"SET ${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key}=true")
      runJobLightQueries(queryLocation = "job-light", queries = queriesToRun, tableSizes)
    }

    println("complete")
    // scalastyle:on println
    while (true) {

    }
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

  def runJobLightQueries(queryLocation: String,
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
      val benchmark = new Benchmark("Job Light Snappy", numRows, 1, output = output)
      benchmark.addCase(s"$name$nameSuffix") { _ =>
        spark.sql(queryString).noop()
      }
      benchmark.run()
    }
  }
}
