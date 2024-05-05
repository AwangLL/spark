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
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.ExtendedSQLTest

@ExtendedSQLTest
class JOBLightQuerySuite extends BenchmarkQueryTest with JOBLightBase {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.READ_SIDE_CHAR_PADDING, false)

  jobLightAllQueries.foreach {name =>
    val queryString = resourceToString(s"job-light/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    test(s"$name") {
      val plan = sql(queryString).queryExecution.executedPlan
      checkGeneratedCode(plan)
    }
  }
}

@ExtendedSQLTest
class JOBLightQueryWithStatsSuite extends JOBLightQuerySuite {
  override def injectStats: Boolean = true
}

@ExtendedSQLTest
class JOBLightQueryANSISuite extends JOBLightQuerySuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}