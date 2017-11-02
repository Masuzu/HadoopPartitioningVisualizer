import org.json4s.JsonDSL._
import org.json4s._

// import <other json helpers

case class PartitionerStats(
    mapperId: Int,
    sizePerPartition: Map[Int, Long]) extends JsonSerializable

case object PartitionerStatsFormat extends CustomFormat[PartitionerStats] {
  override def deserializeImpl(formats: Formats, ast: JValue): PartitionerStats = {
    val sizePerPartition = (ast \ "sizePerPartition").extract[Map[Int, Long]]
    PartitionerStats(
      (ast \ "mapperId").extract[Int],
      sizePerPartition
    )
  }

  override def serializeImpl(formats: Formats, obj: PartitionerStats): JValue = {
    ("mapperId" -> obj.mapperId) ~
      ("sizePerPartition" -> Extraction.decompose(obj.sizePerPartition))
  }
}

case class AggregatedPartitionerStats(statsPerMapper:List[PartitionerStats]) extends JsonSerializable

case object AggregatedPartitionerStatsFormat extends CustomFormat[AggregatedPartitionerStats] {
  override def deserializeImpl(formats: Formats, ast: JValue): AggregatedPartitionerStats =
    AggregatedPartitionerStats(ast.extract[List[PartitionerStats]])

  override def serializeImpl(formats: Formats, obj: AggregatedPartitionerStats): JValue =
    Extraction.decompose(obj.statsPerMapper)
}