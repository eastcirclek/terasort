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

import scala.util.Try

class FlinkTeraPartitioner(underlying:TotalOrderPartitioner) extends Partitioner[Text] {
  def partition(key:Text, numPartitions:Int):Int = {
    underlying.getPartition(key)
  }
}

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
    if(args.size != 6){
      println("Usage: FlinkTeraSort hdfs inputPath outputPath #partitions " +
              "useOptimizedText(true or false) " +
              "useObjectReuse(true or false)") 
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val hdfs = args(0)
    val inputPath= hdfs+args(1)
    val outputPath = hdfs+args(2)
    val partitions = args(3).toInt
    val useOptimizedText:Boolean = Try(args(4).toBoolean).getOrElse(true)
    val useObjectReuse:Boolean = Try(args(5).toBoolean).getOrElse(true)
    println("useOptimizedText : " + useOptimizedText);
    println("useObjectReuse : " + useObjectReuse)
    if(useObjectReuse){
      env.getConfig.enableObjectReuse()
    }

    val mapredConf = new JobConf()
    mapredConf.set("fs.defaultFS", hdfs)
    mapredConf.set("mapreduce.input.fileinputformat.inputdir", inputPath)
    mapredConf.set("mapreduce.output.fileoutputformat.outputdir", outputPath)
    mapredConf.setInt("mapreduce.job.reduces", partitions)

    val jobContext = Job.getInstance(mapredConf)

    val partitionFile = new Path(outputPath, TeraInputFormat.PARTITION_FILENAME)
    TeraInputFormat.writePartitionFile(jobContext, partitionFile)
    val underlyingPartitioner = new TotalOrderPartitioner(mapredConf, partitionFile)
    val teraInputFormat = new TeraInputFormat()
    val teraOutputFormat = new TeraOutputFormat()
    val hadoopOF = new HadoopOutputFormat[Text, Text](teraOutputFormat, jobContext);

    if(useOptimizedText){
      val partitioner = new OptimizedFlinkTeraPartitioner(underlyingPartitioner)

      val inputFile = env.readHadoopFile(teraInputFormat, classOf[Text], classOf[Text], inputPath)
      val optimizedText = inputFile.map(tp => (new OptimizedText(tp._1), tp._2))
      val sortedPartitioned = optimizedText.partitionCustom(partitioner, 0).sortPartition(0, Order.ASCENDING)
      sortedPartitioned.map(tp => (tp._1.getText, tp._2)).output(hadoopOF)
      env.execute("TeraSort")
    }else{
      val partitioner = new FlinkTeraPartitioner(underlyingPartitioner)
      
      val inputFile = env.readHadoopFile(teraInputFormat, classOf[Text], classOf[Text], inputPath)
      val sortedPartitioned = inputFile.partitionCustom(partitioner, 0).sortPartition(0, Order.ASCENDING)
      sortedPartitioned.output(hadoopOF)
      env.execute("TeraSort")
    }
  }
}
