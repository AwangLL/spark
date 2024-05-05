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

trait JOBLightSchema {
  protected val tableColumns: Map[String, String] = Map(
    "cast_info" ->
      """
        |`id` INT,
        |`person_id` INT,
        |`movie_id` INT,
        |`person_role_id` INT,
        |`note` STRING,
        |`nr_order` INT,
        |`role_id` INT
    """.stripMargin,
    "movie_companies" ->
      """
        |`id` INT,
        |`movie_id` INT,
        |`company_id` INT,
        |`company_type_id` INT,
        |`note` STRING
    """.stripMargin,
    "movie_info_idx" ->
      """
        |`id` INT,
        |`movie_id` INT,
        |`info_type_id` INT,
        |`info` STRING,
        |`note` VARCHAR(1)
    """.stripMargin,
    "movie_keyword" ->
      """
        |`id` INT,
        |`movie_id` INT,
        |`keyword_id` INT
    """.stripMargin,
    "title" ->
      """
        |`id` INT,
        |`title` STRING,
        |`imdb_index` VARCHAR(5),
        |`kind_id` INT,
        |`production_year` INT,
        |`imdb_id` INT,
        |`phonetic_code` VARCHAR(5),
        |`episode_of_id` INT,
        |`season_nr` INT,
        |`episode_nr` INT,
        |`series_years` VARCHAR(49),
        |`md5sum` VARCHAR(32)
    """.stripMargin,
    "movie_info" ->
      """
        |`id` INT,
        |`movie_id` INT,
        |`info_type_id` INT,
        |`info` STRING,
        |`note` STRING
    """.stripMargin,
  )

  protected val tablePartitionColumns = Map(
    "cast_info" -> Seq("`id`"),
    "movie_companies" -> Seq("`id`"),
    "movie_info_idx" -> Seq("`id`"),
    "movie_keyword" -> Seq("`id`"),
    "title" -> Seq("`id`"),
    "movie_info" -> Seq("`id`")
  )
}
