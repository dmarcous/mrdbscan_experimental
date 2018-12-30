#!/usr/bin/env bash

# Usage:
# --max_points_per_partition
# --input_remote_dir
# --output_remote_dir
# --experiment_index
# --local_exp_dir

# Experiment setup
MINPTS=20
EPSILON=10

# Set args defaults
LOCAL="/mnt/experiments/"
INPUT_REMOTE="s3://mybucket/data/"
OUTPUT_REMOTE="s3://mybucket/output/"
MAXPTS=250
INDEX=0

# Read params
echo 'Reading script params...'
#while [ $# -gt 0 ]; do
for i in "$@"
do
case $i in
--max_points_per_partition=*)
MAXPTS="${i#*=}"
shift
;;
--input_remote_dir=*)
INPUT_REMOTE="${i#*=}"
shift
;;
--output_remote_dir=*)
OUTPUT_REMOTE="${i#*=}"
shift
;;
--experiment_index=*)
INDEX="${i#*=}"
shift
;;
--local_exp_dir=*)
LOCAL="${i#*=}"
shift
;;
-*)
# do not exit out, just note failure
echo "unrecognized option: ${i#*=}"
;;
*)
break;
;;
esac
shift
done
echo 'Running with parameters : '
echo "INPUT_REMOTE = ${INPUT_REMOTE}"
echo "OUTPUT_REMOTE = ${OUTPUT_REMOTE}"
echo "LOCAL = ${LOCAL}"
echo "MAXPTS = ${MAXPTS}"
echo "MINPTS = ${MINPTS}"
echo "EPSILON = ${EPSILON}"
echo "INDEX = ${INDEX}"

# Set useful variables
JAR_PATH="/resources/jar/mrdbscan_experimental_2.11-1.0.0.jar"
CURRENT_EXP_OUTPUT=$OUTPUT_REMOTE/MRDBSCAN/part_$MAXPTS/exp_$INDEX/

echo "Preparing run cmd"
RUN_CMD="/usr/lib/spark/bin/spark-submit --class org.apache.spark.mllib.clustering.dbscan.CLIRunner --driver-java-options='-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop' --conf spark.default.parallelism=64 ${LOCAL}${JAR_PATH} --inputFilePath ${INPUT_REMOTE} --outputFolderPath ${CURRENT_EXP_OUTPUT} --positionFieldLon 1 --positionFieldLat 2 --inputFieldDelimiter , --epsilon ${EPSILON} --minPts ${MINPTS} --maxPointsPerPartition ${MAXPTS}"
echo ${RUN_CMD}

echo "Starting run"
`${RUN_CMD}`
