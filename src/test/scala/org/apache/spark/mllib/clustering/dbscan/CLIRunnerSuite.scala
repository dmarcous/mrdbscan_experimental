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

import java.io.File

import com.github.dmarcous.ddbgscan.api.RuntimeConfig
import com.github.dmarcous.ddbgscan.core.config.{AlgorithmParameters, IOConfig}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

class CLIRunnerSuite extends FunSuite with Matchers {

  val epsilon = 100.0
  val minPts = 3
  val maxPointsPerPartition = 999
  val parameters = AlgorithmParameters(
    epsilon,
    minPts
  )
  val inputPath = "./src/test/resources/geo_data.csv"
  val outputFolderPath = "/tmp/MRDBGSCAN/"
  val ioConfig = IOConfig(
    inputPath,
    outputFolderPath
  )
  val conf =
    RuntimeConfig(
      ioConfig,
      parameters
    )

  val args =
    Array(
      "--inputFilePath",inputPath,"--outputFolderPath",outputFolderPath,
      "--epsilon",epsilon.toString,"--minPts",minPts.toString,
      "--maxPointsPerPartition",maxPointsPerPartition.toString
    )

  test("should parse args") {

    println("args")
    println(args.foreach(println))
    val parsedConf = CLIRunner.parseArgs(args)
    (conf,maxPointsPerPartition) should equal(parsedConf)

  }

  test("should cluster full pipeline") {

    // Create outside test so test will get it from here
    val spark =
      SparkSession
        .builder()
        .master("local")
        .appName("CLIRunnerTest")
        .getOrCreate()

    // Clean output before run
    FileUtils.deleteQuietly(new File(outputFolderPath))

    // Run algorithm
    CLIRunner.main(args)

  }
}
