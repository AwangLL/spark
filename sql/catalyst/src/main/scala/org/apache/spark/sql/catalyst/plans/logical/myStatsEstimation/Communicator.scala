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

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket


object Communicator {
  def socket(data: Array[Byte]): String = {
    val host = "127.0.0.1"
    val port = 8000

    try {
      val socket = new Socket(host, port)
      val outputStream = socket.getOutputStream
      outputStream.write(data)

      val inputStream = socket.getInputStream
      val reader = new BufferedReader(new InputStreamReader(inputStream))
      val response = reader.readLine()
      socket.close()

      response
    } catch {
      case e: Throwable =>
        throw new RuntimeException("Can't connect to the cardinality estimator")
    }
  }
}
