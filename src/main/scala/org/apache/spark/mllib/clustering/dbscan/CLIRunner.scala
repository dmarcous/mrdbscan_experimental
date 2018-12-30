package org.apache.spark.mllib.clustering.dbscan

import com.github.dmarcous.ddbgscan.api.{RuntimeConfig}
import com.github.dmarcous.ddbgscan.core.config.CoreConfig._
import com.github.dmarcous.ddbgscan.core.config.{AlgorithmParameters, IOConfig}
import org.apache.spark.sql.SparkSession

object CLIRunner {
  def main(args: Array[String]) :Unit =
  {
    // Create spark context
    val appName="MRDBSCAN"
    val spark =
      SparkSession
        .builder()
        .appName(appName)
        .getOrCreate()

    val (conf, maxPointsPerPartition) = parseArgs(args)

    // Run clustering main flow algorithm
    MRDBSCANRunner.run(spark, conf, maxPointsPerPartition)
  }

  def parseArgs(args: Array[String]) : (RuntimeConfig, Int) = {
    val usage = """
    Usage: /usr/lib/spark/bin/spark-submit --class org.apache.spark.mllib.clustering.dbscan.CLIRunner [filename.jar]
    --inputFilePath [string] --outputFolderPath [string]
    --epsilon [double] --minPts [int]
    [--positionFieldLon int] [--positionFieldLat int]
    [--inputFieldDelimiter int]
    [--maxPointsPerPartition int]
    """

    var inputPath : String = ""
    var outputFolderPath : String = ""
    var epsilon : Double = Double.NaN
    var minPts : Int = Double.NaN.toInt
    var positionFieldId: Int = NO_UNIQUE_ID_FIELD
    var positionFieldLon: Int = DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER
    var positionFieldLat: Int = DEFAULT_LATITUDE_POSITION_FIELD_NUMBER
    var inputFieldDelimiter : String = DEFAULT_GEO_FILE_DELIMITER
    var maxPointsPerPartition : Int = 250

    args.sliding(2, 2).toList.collect{
      case Array("--inputFilePath", argInputFilePath: String) => inputPath = argInputFilePath
      case Array("--positionFieldLon", argPositionFieldLon: String) => positionFieldLon= argPositionFieldLon.toInt
      case Array("--positionFieldLat", argPositionFieldLat: String) => positionFieldLat = argPositionFieldLat.toInt
      case Array("--inputFieldDelimiter", argOnputFieldDelimiter: String) => inputFieldDelimiter = argOnputFieldDelimiter
      case Array("--outputFolderPath", argOutputFolderPath: String) => outputFolderPath = argOutputFolderPath
      case Array("--epsilon", argEpsilon: String) => epsilon = argEpsilon.toDouble
      case Array("--minPts", argMinPts: String) => minPts = argMinPts.toInt
      case Array("--maxPointsPerPartition", argMaxPointsPerPartition: String) => maxPointsPerPartition = argMaxPointsPerPartition.toInt
    }

    // Make sure all mandatory params are in place
    if (inputPath.isEmpty || outputFolderPath.isEmpty || epsilon.isNaN || minPts.isNaN)
    {
      println(usage)
      System.exit(1)
    }

    println("inputPath : " + inputPath)
    println("positionFieldLon : " + positionFieldLon)
    println("positionFieldLat : " + positionFieldLat)
    println("inputFieldDelimiter : " + inputFieldDelimiter)
    println("outputFolderPath : " + outputFolderPath)
    println("epsilon : " + epsilon)
    println("minPts : " + minPts)
    println("maxPointsPerPartition : " + maxPointsPerPartition)
    (
      RuntimeConfig(
        IOConfig(
          inputPath,
          outputFolderPath,
          positionFieldId,
          positionFieldLon,
          positionFieldLat,
          inputFieldDelimiter
        ),
        AlgorithmParameters(
          epsilon,
          minPts
        )
      ),
      maxPointsPerPartition
    )
  }

}
