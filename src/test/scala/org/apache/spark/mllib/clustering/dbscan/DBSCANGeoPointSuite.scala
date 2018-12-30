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
package org.apache.spark.mllib.clustering.dbscan

import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.{FunSuite, Matchers}

class DBSCANGeoPointSuite extends FunSuite with Matchers {
  test("should calc geo dist") {

    val pnt1 = DBSCANGeoPoint(Vectors.dense(34.777113, 32.0718014))
    val pnt2 = DBSCANGeoPoint(Vectors.dense(34.777547,32.072729))

    pnt1.geoDistance(pnt2) should equal(111.07939530820765)

  }

  test("should return bunding box") {

    val pnt1 = DBSCANGeoPoint(Vectors.dense(34.777113, 32.0718014))
    val bb1 = pnt1.getBoundingBox(20)
    bb1 should equal(DBSCANRectangle(
      34.77693333694317,32.07162173694317,
      34.777292663056826,32.071981063056825))

    val pnt2 = DBSCANGeoPoint(Vectors.dense(-74.0161377, 40.6899442))
    val bb2 = pnt2.getBoundingBox(20)
    bb2 should equal(DBSCANRectangle(
      -74.01631736305683,40.68976453694317,
      -74.01595803694318,40.690123863056826))

  }

  test("should correctly shrink bunding box") {

    val pnt1 = DBSCANGeoPoint(Vectors.dense(34.777113, 32.0718014))
    val bb1 = pnt1.getBoundingBox(20)
    val bb_shrinked = bb1.shrink(10)
    println(bb_shrinked)
    bb_shrinked should equal(DBSCANRectangle(
      34.77702316847159,32.071711568471585,
      34.77720283152841,32.07189123152841))
  }
}
