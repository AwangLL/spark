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

import java.util.Locale


class JOBSingleQueryBenchmarkArguments(val args: Array[String]) {
  var dataLocation: String = sys.env.getOrElse("SPARK_IMDB_DATA", null)
  var query: Seq[String] = Seq.empty
  var rboEnabled: Boolean = false
  var cboEnabled: Boolean = false
  var mycboEnabled: Boolean = false

  parseArgs(args.toList)
  validateArguments()

  private def optionMatch(optionName: String, s: String): Boolean = {
    optionName == s.toLowerCase(Locale.ROOT)
  }

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {
        case optName :: value :: tail if optionMatch("--data-location", optName) =>
          dataLocation = value
          args = tail

        case optName :: value :: tail if optionMatch("--query", optName) =>
          query = value.toLowerCase(Locale.ROOT).split(",").map(_.trim).toSeq
          args = tail

        case optName :: tail if optionMatch("--rbo", optName) =>
          rboEnabled = true
          args = tail

        case optName :: tail if optionMatch("--cbo", optName) =>
          cboEnabled = true
          args = tail

        case optName :: tail if optionMatch("--mycbo", optName) =>
          mycboEnabled = true
          args = tail

        case _ =>
          // scalastyle:off println
          System.err.println("Unknown/unsupported param " + args)
          // scalastyle:on println
          printUsageAndExit(1)
      }
    }
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off
    System.err.println(
      """
        |Usage: spark-submit --class <this class> <spark sql test jar> [Options]
        |Options:
        |  --data-location      Path to IMDB data
        |  --query              Queries to execute, e.g., q3,q5,q13
        |  --rbo                Whether to enable rule-based optimization
        |  --cbo                Whether to enable cost-based optimization
        |  --mycbo              Whether to enable my own cardinality estimator
        |
        |------------------------------------------------------------------------------------------------------------------
        |In order to run this benchmark, please follow the instructions at
        |https://github.com/gregrahn/join-order-benchmark/README.md
      """.stripMargin)
    // scalastyle:on
    System.exit(exitCode)
  }

  private def validateArguments(): Unit = {
    if (dataLocation == null) {
      // scalastyle:off println
      System.err.println("Must specify a data location")
      // scalastyle:on println
      printUsageAndExit(-1)
    }

    if (query == null) {
      // scalastyle:off println
      System.err.println("Must provide at least a query")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
  }
}
