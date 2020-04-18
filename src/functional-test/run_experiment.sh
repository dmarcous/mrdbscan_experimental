#!/usr/bin/env bash

# Usage:
# --max_points_per_partition
# --input_remote_dir
# --output_remote_dir
# --experiment_index
# --local_exp_dir
# --parallelism
# --minpts
# --epsilon

# Experiment setup
MINPTS=20
EPSILON=40

# Set args defaults
LOCAL="/mnt/experiments/"
INPUT_REMOTE="s3://mybucket/data/"
OUTPUT_REMOTE="s3://mybucket/output/"
INDEX=0
PARALLELISM=256
NUM_PARTITIONS=256
PARTITIONING_STATEGY="cost"
DEBUG="false"
MAX_POINTS_PER_PARTITION=256
DRIVER_MEMORY="1g"

# Read params
echo 'Reading script params...'
#while [ $# -gt 0 ]; do
for i in "$@"
do
case $i in
--neighborhood_partitioning_lvl=*)
PARTITION_LVL="${i#*=}"
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
--parallelism=*)
PARALLELISM="${i#*=}"
shift
;;
--numPartitions=*)
NUM_PARTITIONS="${i#*=}"
shift
;;
--minpts=*)
MINPTS="${i#*=}"
shift
;;
--epsilon=*)
EPSILON="${i#*=}"
shift
;;
--partitioningStrategy=*)
PARTITIONING_STATEGY="${i#*=}"
shift
;;
--debug=*)
DEBUG="${i#*=}"
shift
;;
--maxPointsPerPartition=*)
MAX_POINTS_PER_PARTITION="${i#*=}"
shift
;;
--driverMemory=*)
DRIVER_MEMORY="${i#*=}"
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
echo "MINPTS = ${MINPTS}"
echo "EPSILON = ${EPSILON}"
echo "INDEX = ${INDEX}"
echo "PARALLELISM = ${PARALLELISM}"
echo "NUM_PARTITIONS = ${NUM_PARTITIONS}"
echo "PARTITIONING_STATEGY = ${PARTITIONING_STATEGY}"
echo "MAX_POINTS_PER_PARTITION = ${MAX_POINTS_PER_PARTITION}"
echo "DRIVER_MEMORY = ${DRIVER_MEMORY}"
echo "DEBUG = ${DEBUG}"

# Set useful variables
JAR_PATH="/resources/jar/mrdbscan_experimental_2.11-2.4.3_1.0.0.jar"
CURRENT_EXP_OUTPUT=$OUTPUT_REMOTE/mrdbscan/cost/partlvl_99/maxp_$MAX_POINTS_PER_PARTITION/exp_$INDEX/

echo "Preparing run cmd"
RUN_CMD="/usr/lib/spark/bin/spark-submit --class org.apache.spark.mllib.clustering.dbscan.CLIRunner --driver-java-options='-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop' --conf spark.default.parallelism=${PARALLELISM} --conf spark.driver.maxResultSize=4g ${LOCAL}${JAR_PATH} --inputFilePath ${INPUT_REMOTE} --outputFolderPath ${CURRENT_EXP_OUTPUT} --positionFieldId 0 --positionFieldLon 1 --positionFieldLat 2 --inputFieldDelimiter , --epsilon ${EPSILON} --minPts ${MINPTS} --maxPointsPerPartition ${MAX_POINTS_PER_PARTITION}"
echo ${RUN_CMD}

echo "Starting run"
`${RUN_CMD}`
