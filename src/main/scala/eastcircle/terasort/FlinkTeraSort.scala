package eastcircle.terasort

import org.apache.flink.api.scala._
import org.apache.flink.api.common.JobExecutionResult

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.operators.Order

import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job

class FlinkTeraPartitioner(underlying:TotalOrderPartitioner) extends Partitioner[Text] {
  def partition(key:Text, numPartitions:Int):Int = {
    underlying.getPartition(key)
  }
}

object FlinkTeraSort {

  implicit val textOrdering = new Ordering[Text] {
    override def compare(a:Text, b:Text) = a.compareTo(b)
  }

  def main(args: Array[String]){
    val env = ExecutionEnvironment.getExecutionEnvironment

    val hdfs = args(0)
    val inputPath= hdfs+args(1)
    val outputPath = hdfs+args(2)
    val partitions = args(3).toInt
    
    val mapredConf = new JobConf()
    mapredConf.set("fs.defaultFS", hdfs)
    mapredConf.set("mapreduce.input.fileinputformat.inputdir", inputPath)
    mapredConf.set("mapreduce.output.fileoutputformat.outputdir", outputPath)
    mapredConf.setInt("mapreduce.job.reduces", partitions)

    val jobContext = Job.getInstance(mapredConf)

    val partitionFile = new Path(outputPath, TeraInputFormat.PARTITION_FILENAME)
    TeraInputFormat.writePartitionFile(jobContext, partitionFile)
    val partitioner = new FlinkTeraPartitioner(new TotalOrderPartitioner(mapredConf, partitionFile))
    
    val teraInputFormat = new TeraInputFormat()
    val teraOutputFormat = new TeraOutputFormat()
    val hadoopOF = new HadoopOutputFormat[Text, Text](teraOutputFormat, jobContext);

    val inputFile = env.readHadoopFile(teraInputFormat, classOf[Text], classOf[Text], inputPath)
    inputFile.partitionCustom(partitioner, 0).sortPartition(0, Order.ASCENDING).output(hadoopOF)
    env.execute("TeraSort")
  }
}
