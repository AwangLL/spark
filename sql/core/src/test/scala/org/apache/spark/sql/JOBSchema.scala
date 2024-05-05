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

trait JOBSchema {
  protected val tableColumns: Map[String, String] = Map(
    "aka_name" ->
      """
        |`id` INT,
        |`person_id` INT,
        |`name` STRING,
        |`imdb_index` VARCHAR(3),
        |`name_pcode_cf` VARCHAR(11),
        |`name_pcode_nf` VARCHAR(11),
        |`surname_pcode` VARCHAR(11),
        |`md5sum` VARCHAR(65)
    """.stripMargin,
    "aka_title" ->
      """
        |`id` INT,
        |`movie_id` INT,
        |`title` STRING,
        |`imdb_index` VARCHAR(4),
        |`kind_id` INT,
        |`production_year` INT,
        |`phonetic_code` VARCHAR(5),
        |`episode_of_id` INT,
        |`season_nr` INT,
        |`episode_nr` INT,
        |`note` VARCHAR(72),
        |`md5sum` VARCHAR(32)
    """.stripMargin,
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
    "char_name" ->
      """
        |`id` INT,
        |`name` STRING,
        |`imdb_index` VARCHAR(2),
        |`imdb_id` INT,
        |`name_pcode_nf` VARCHAR(5),
        |`surname_pcode` VARCHAR(5),
        |`md5sum` VARCHAR(32)
    """.stripMargin,
    "comp_cast_type" ->
      """
        |`id` INT,
        |`kind` VARCHAR(32)
    """.stripMargin,
    "company_name" ->
      """
        |`id` INT,
        |`name` STRING,
        |`country_code` VARCHAR(6),
        |`imdb_id` INT,
        |`name_pcode_nf` VARCHAR(5),
        |`name_pcode_sf` VARCHAR(5),
        |md5sum VARCHAR(32)
    """.stripMargin,
    "company_type" ->
      """
        |`id` INT,
        |`kind` VARCHAR(32)
    """.stripMargin,
    "complete_cast" ->
      """
        |`id` INT,
        |`movie_id` INT,
        |`subject_id` INT,
        |`status_id` INT
    """.stripMargin,
    "info_type" ->
      """
        |`id` INT,
        |`info` VARCHAR(32)
    """.stripMargin,
    "keyword" ->
      """
        |`id` INT,
        |`keyword` STRING,
        |`phonetic_code` VARCHAR(5)
    """.stripMargin,
    "kind_type" ->
      """
        |`id` INT,
        |`kind` VARCHAR(15)
    """.stripMargin,
    "link_type" ->
      """
        |`id` INT,
        |`link` VARCHAR(32)
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
    "movie_link" ->
      """
        |`id` INT,
        |`movie_id` INT,
        |`linked_movie_id` INT,
        |`link_type_id` INT
    """.stripMargin,
    "name" ->
      """
        |`id` INT,
        |`name` STRING,
        |`imdb_index` VARCHAR(9),
        |`imdb_id` INT,
        |`gender` VARCHAR(1),
        |`name_pcode_cf` VARCHAR(5),
        |`name_pcode_nf` VARCHAR(5),
        |`surname_pcode` VARCHAR(5),
        |`md5sum` VARCHAR(32)
    """.stripMargin,
    "role_type" ->
      """
        |`id` INT,
        |`role` VARCHAR(32)
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
    "person_info" ->
      """
        |`id` INT,
        |`person_id` INT,
        |`info_type_id` INT,
        |`info` STRING,
        |`note` STRING
    """.stripMargin
  )

  protected val tablePartitionColumns = Map(
    "aka_name" -> Seq("`id`"),
    "aka_title" -> Seq("`id`"),
    "cast_info" -> Seq("`id`"),
    "char_name" -> Seq("`id`"),
    "comp_cast_type" -> Seq("`id`"),
    "company_name" -> Seq("`id`"),
    "company_type" -> Seq("`id`"),
    "complete_cast" -> Seq("`id`"),
    "info_type" -> Seq("`id`"),
    "keyword" -> Seq("`id`"),
    "kind_type" -> Seq("`id`"),
    "link_type" -> Seq("`id`"),
    "movie_companies" -> Seq("`id`"),
    "movie_info_idx" -> Seq("`id`"),
    "movie_keyword" -> Seq("`id`"),
    "movie_link" -> Seq("`id`"),
    "name" -> Seq("`id`"),
    "role_type" -> Seq("`id`"),
    "title" -> Seq("`id`"),
    "movie_info" -> Seq("`id`"),
    "person_info" -> Seq("`id`"),
  )
}
