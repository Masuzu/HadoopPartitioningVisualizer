import org.json4s.Formats

package object utils {
  implicit val fmt: Formats = org.json4s.DefaultFormats +
    PartitionerStatsFormat +
    AggregatedPartitionerStatsFormat
}