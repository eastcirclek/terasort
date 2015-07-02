# terasort

TeraSort for Apache Spark and Flink.

# Build

`sbt package`

# Run TeraSort using Spark on Yarn

## use your values for the following variables
  EXECUTORS=42 <br />
  EXECUTOR_CORES=12 <br />
  EXECUTOR_MEMORY=10G <br />
  SCALA_VERSION=2.10 <br />
  HDFS=hdfs://master:54321 <br />
  INDIR=/nodedata_80gb/blk_256mb <br />
  OUTDIR=/spark_out <br />
  PARTITIONS=120 <br />

## run the following command 
  `spark-submit --master yarn\ <br />`
    --num-executors ${EXECUTORS}\ <br />
    --executor-cores ${EXECUTOR_CORES}\ <br />
    --executor-memory ${EXECUTOR_MEMORY}\ <br />
    --class eastcircle.terasort.SparkTeraSort\ <br />
    target/${SCALA_VERSION}/terasort_${SCALA_VERSION}-0.0.1.jar\ <br />
    ${HDFS} ${INDIR} ${OUTDIR} ${PARTITIONS} <br />


# Run TeraSort using Flink on Yarn
