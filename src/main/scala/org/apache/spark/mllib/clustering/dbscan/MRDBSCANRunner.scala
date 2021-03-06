package org.apache.spark.mllib.clustering.dbscan

import com.github.dmarcous.ddbgscan.api.RuntimeConfig
import org.apache.spark.mllib.clustering.dbscan.DBSCANLabeledPoint.Flag
import com.github.dmarcous.ddbgscan.core.config.CoreConfig.ClusteringInstanceStatusValue.{BORDER, CORE, NOISE, UNKNOWN}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.PairRDDFunctions

object MRDBSCANRunner{

  def run(@transient spark: SparkSession, conf: RuntimeConfig, maxPointsPerPartition: Int) :Unit =
  {
    // Read input file
    val data =
      spark.read
        .textFile(conf.ioConfig.inputPath).rdd

    // Extract geo data from input and keep rest
    println("Preprocessing...")
    DBSCAN.setJobStageNameInSparkUI(spark, "Preprocessing",
      "Stage 0 - Create clustering instances dataset keyed by geo")
    val parsedData =
      data.map(_.split(','))
          .map{case(fields) =>
            Vectors.dense(
              Array(fields(conf.ioConfig.positionLon).toDouble,
                    fields(conf.ioConfig.positionLat).toDouble))}

    // Run clustering algorithm
    println("Starting clustering...")
    val model = DBSCAN.train(
      spark,
      parsedData,
      conf.parameters.epsilon,
      conf.parameters.minPts,
      maxPointsPerPartition)

    val results =
      model.labeledPoints.map { p => (
        (p.x.toString + "|" + p.y.toString),
        //         p.x.toString + "|" + p.y.toString,
        (p.cluster,
          if (p.flag == Flag.Core) CORE.value
          else if (p.flag == Flag.Border) BORDER.value
          else if (p.flag == Flag.Noise) NOISE.value
          else UNKNOWN.value
        )
      )}
    val identifiableData =
      data.map(_.split(','))
        .map{case(fields) =>
          (
            (fields(conf.ioConfig.positionLon).toDouble.toString + "|" +
           fields(conf.ioConfig.positionLat).toDouble.toString),
            (fields(conf.ioConfig.positionId).toLong))}
    val identifiableResults =
      results.join(identifiableData)
          .map{p => (p._2._2, p._2._1._1, p._2._1._2)}


    // Trigger ops before output for time measurements
    identifiableResults.count()

    // Write output
    println("Writing results...")
    DBSCAN.setJobStageNameInSparkUI(spark, "Output",
      "Stage 4 - Writing output as CSV")
    identifiableResults.saveAsTextFile(conf.ioConfig.outputFolderPath)
  }
}
