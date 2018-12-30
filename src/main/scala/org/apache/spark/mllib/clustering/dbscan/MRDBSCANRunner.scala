package org.apache.spark.mllib.clustering.dbscan

import com.github.dmarcous.ddbgscan.api.RuntimeConfig
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

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
      data.map(s => Vectors.dense(s.split(',').map(_.toDouble)))

    // Run clustering algorithm
    println("Starting clustering...")
    val model = DBSCAN.train(
      spark,
      parsedData,
      conf.parameters.epsilon,
      conf.parameters.minPts,
      maxPointsPerPartition)

    val results =
      model.labeledPoints.map(p => (
        (p.x.toString + "|" + p.y.toString),
         p.cluster,
         p.flag.id)

    // Write output
    println("Writing results...")
    DBSCAN.setJobStageNameInSparkUI(spark, "Output",
      "Stage 4 - Writing output as CSV")
    results.saveAsTextFile(conf.ioConfig.outputFolderPath)
  }
}
