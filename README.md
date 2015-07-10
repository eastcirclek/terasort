# terasort

TeraSort for Apache Spark and Flink.

# Build

`sbt package`

# Run TeraSort using Spark on Yarn

The below shows an example of a bash script to execute TeraSort on Spark on top of Yarn.

```
#!/usr/bin/env bash

EXECUTORS=42
EXECUTOR_CORES=12
EXECUTOR_MEMORY=10G
SCALA_VERSION=2.10
HDFS=hdfs://master:54321
INDIR=/nodedata_80gb/blk_256mb
OUTDIR=/spark_out
PARTITIONS=$[42*12]

spark-submit --master yarn\
--num-executors ${EXECUTORS}\
--executor-cores ${EXECUTOR_CORES}\
--executor-memory ${EXECUTOR_MEMORY}\
--class eastcircle.terasort.SparkTeraSort\
target/${SCALA_VERSION}/terasort_${SCALA_VERSION}-0.0.1.jar\
${HDFS} ${INDIR} ${OUTDIR} ${PARTITIONS}
```


# Run TeraSort using Flink on Yarn

## execute job manager and task managers

The below shows an example of a command to execute job manager and 42 task managers.

```
yarn-session.sh -n 42 -tm 10240 -s 12 -tmc 12
```

yarn-session.sh will show you the address of job manager via stdout as follows:
```
...
Flink JobManager is now running on slave1:33970
...
```

## Submit a TeraSort job to job manager 

The below shows an example of a bash script to execute TeraSort on Flink.

```
#!/usr/bin/env bash

JOBMANAGER=slave1:33970
PARTITIONS=$[42*12]
SCALA_VERSION=2.10
HDFS=hdfs://master:54321
INDIR=/nodedata_80gb/blk_256mb
OUTDIR=/flink_out
USE_OPTIMIZED_TEXT=true
USE_OBJECT_REUSE=true

flink run\
-m ${JOBMANAGER}\
-p ${PARTITIONS}\
-c eastcircle.terasort.FlinkTeraSort\
target/${SCALA_VERSION}/terasort_${SCALA_VERSION}-0.0.1.jar\
${HDFS} ${INDIR} ${OUTDIR} ${PARTITIONS} ${USE_OPTIMIZED_TEXT} ${USE_OBJECT_REUSE}

```
