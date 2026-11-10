package qnview

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// SegmentDistribution reports actual loaded bases, including bases retained by
// older Views. Requirements alone do not prove that their artifacts are loaded.
func (m *SegmentResourceManager) SegmentDistribution() []*querypb.SegmentVersionInfo {
	var result []*querypb.SegmentVersionInfo
	m.inspectPhysicalSegments(0, func(s segments.Segment) {
		loadInfo := s.LoadInfo()
		indexes := make(map[int64]*querypb.FieldIndexInfo)
		for _, index := range s.Indexes() {
			indexes[index.IndexInfo.GetIndexID()] = index.IndexInfo
		}
		info := &querypb.SegmentVersionInfo{
			ID: s.ID(), Collection: s.Collection(), Partition: s.Partition(),
			Channel: s.Shard().VirtualName(), Version: s.Version(),
			Level: s.Level(), IsSorted: s.IsSorted(), LastDeltaTimestamp: s.LastDeltaTimestamp(),
			IndexInfo: indexes, JsonStatsInfo: s.GetFieldJSONIndexStats(),
			ManifestPath: loadInfo.GetManifestPath(), DataVersion: proto.Int32(loadInfo.GetDataVersion()),
		}
		// Segment teardown may follow as soon as inspection returns.
		result = append(result, proto.Clone(info).(*querypb.SegmentVersionInfo))
	})
	return result
}
