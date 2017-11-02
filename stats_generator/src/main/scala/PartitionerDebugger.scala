import java.io.PrintWriter
import java.nio.file.Path
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.io.{Writable, WritableComparator}
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

// import <other helpers>

/**
  * A helper class which tracks the input size in bytes per reducer and input group.
  * @param computeInputGroupSizes If set to true, compute the size of values per reducer input group
  * @tparam K key type
  * @tparam V value type
  */
case class PartitionerDebugger[K <: Writable, V <: Writable](
    groupingComparator: WritableComparator,
    computeInputGroupSizes: Boolean = false)(
      getPartition: (K, V) => Int,
      getValueSizeInBytes: V => Long
    ) {

  val inputSizeByPartition: mutable.Map[Int, Long] = mutable.Map.empty
  val inputSizeByInputGroup: util.TreeMap[K, Long] = new java.util.TreeMap[K, Long](new Ordering[K] {
    override def compare(key1: K, key2: K): Int = {
      groupingComparator.compare(key1, key2)
    }
  })

  def newKeyValuePair(key: K, value: V): Unit = {
    val valueSizeInBytes = getValueSizeInBytes(value)
    Try[Int] {
      getPartition(key, value)
    }.toOption.foreach { partition =>
      inputSizeByPartition += (partition ->
        (inputSizeByPartition.getOrElse(partition, 0L) + valueSizeInBytes))
    }
    if (computeInputGroupSizes) {
      inputSizeByInputGroup.put(key, inputSizeByInputGroup.getOrDefault(key, 0L) + valueSizeInBytes)
    }
  }

  def printStats(): Unit = {
    inputSizeByPartition.foreach { case (partition, size) => println(s"Partition $partition: $size bytes")}
    if (computeInputGroupSizes) {
      inputSizeByInputGroup.asScala.foreach { case (key, size) => println(s"${key.toString}: $size bytes") }
    }
  }

  def dumpStatsToHdfs(context: TaskInputOutputContext[_, _, _, _])(hdfs: Hdfs): Unit = {
    val mapperId = context.getTaskAttemptID.getTaskID.getId
    val outputPath = FileOutputFormat.getOutputPath(context)
    val partitionerStatsDirectory = outputPath / "partitionerStats"
    hdfs.mkdir(partitionerStatsDirectory.toPath)

    val statsFile = partitionerStatsDirectory / s"stats_$mapperId"
    if (hdfs.exists(statsFile.toPath)) {
      hdfs.delete(statsFile.toPath)
    }
    hdfs.write(PartitionerStats(mapperId, inputSizeByPartition.toMap).toJson, statsFile.toPath)
  }
}

object PartitionerDebugger {
  def aggregateStats(statsOutputFolder: Path)(hdfs: Hdfs): AggregatedPartitionerStats =
    AggregatedPartitionerStats(hdfs.listFiles(statsOutputFolder / "*").flatMap { file =>
      println(s"Reading ${file.path.toString}")
      hdfs.readJsonOpt[PartitionerStats](file.path)
    })
}