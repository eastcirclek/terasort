package eastcircle.terasort

import org.apache.flink.api.scala._

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.operators.Order

import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job

class OptimizedFlinkTeraPartitioner(underlying:TotalOrderPartitioner) extends Partitioner[OptimizedText] {
  def partition(key:OptimizedText, numPartitions:Int):Int = {
    underlying.getPartition(key.getText())
  }
}


object FlinkTeraSort {

  implicit val textOrdering = new Ordering[Text] {
    override def compare(a:Text, b:Text) = a.compareTo(b)
  }

  def main(args: Array[String]){
    if(args.size != 4){
      println("Usage: FlinkTeraSort hdfs inputPath outputPath #partitions ")
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()

    val hdfs = args(0)
    val inputPath= hdfs+args(1)
    val outputPath = hdfs+args(2)
    val partitions = args(3).toInt
    
    val mapredConf = new JobConf()
    mapredConf.set("fs.defaultFS", hdfs)
    mapredConf.set("mapreduce.input.fileinputformat.inputdir", inputPath)
    mapredConf.set("mapreduce.output.fileoutputformat.outputdir", outputPath)
    mapredConf.setInt("mapreduce.job.reduces", partitions)

    val partitionFile = new Path(outputPath, TeraInputFormat.PARTITION_FILENAME)
    val jobContext = Job.getInstance(mapredConf)
    TeraInputFormat.writePartitionFile(jobContext, partitionFile)
    val partitioner = new OptimizedFlinkTeraPartitioner(new TotalOrderPartitioner(mapredConf, partitionFile))
    
    env
      .readHadoopFile(new TeraInputFormat(), classOf[Text], classOf[Text], inputPath)
      .map(tp => (new OptimizedText(tp._1), tp._2))
      .partitionCustom(partitioner, 0).sortPartition(0, Order.ASCENDING)
      .map(tp => (tp._1.getText, tp._2))
      .output(new HadoopOutputFormat[Text, Text](new TeraOutputFormat(), jobContext))
    env.execute("TeraSort")
  }
}
