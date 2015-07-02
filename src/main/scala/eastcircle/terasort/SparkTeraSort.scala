package eastcircle.terasort

import org.apache.spark.SparkContext._

import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, SequenceFile, Text}
import org.apache.spark.{SparkConf, SparkContext, Partitioner}
import org.apache.spark.RangePartitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

class SparkTeraRangePartitioner(underlying:TotalOrderPartitioner,
                                partitions:Int) extends Partitioner {
  def numPartitions: Int = partitions
  def getPartition(key: Any): Int = {
    val textKey = key.asInstanceOf[Text]
    underlying.getPartition(textKey)
  }
}

object SparkTeraSort {

  implicit val textOrdering = new Ordering[Text] {
    override def compare(a: Text, b: Text) = a.compareTo(b)
  }

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("TeraSort")
    val sc = new SparkContext()

    val hdfs = args(0)
    val inputPath = hdfs+args(1)
    val outputPath = hdfs+args(2)
    val partitions = args(3).toInt

    val hadoopConf = new JobConf()
    hadoopConf.set("fs.defaultFS", hdfs)
    hadoopConf.set("mapreduce.input.fileinputformat.inputdir", inputPath)
    hadoopConf.setInt("mapreduce.job.reduces", partitions)

    val partitionFile = new Path(outputPath,
                                 TeraInputFormat.PARTITION_FILENAME)
    val jobContext = Job.getInstance(hadoopConf)
    TeraInputFormat.writePartitionFile(jobContext, partitionFile)

    val inputFile = sc.newAPIHadoopFile[Text, Text, TeraInputFormat](inputPath)
    val partitioner = 
      new SparkTeraRangePartitioner(
          new TotalOrderPartitioner(hadoopConf, partitionFile),
          partitions
      )
    val repartitioned = inputFile.repartitionAndSortWithinPartitions( partitioner )
    repartitioned.saveAsNewAPIHadoopFile[TeraOutputFormat](outputPath)
  }
}
