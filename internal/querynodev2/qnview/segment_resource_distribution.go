package qnview

import (
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// DistributionModifyTS changes on preparation, retirement, catch-up and deletes,
// rather than on polling. It also fences publication of an empty manager.
func (m *SegmentResourceManager) DistributionModifyTS() int64 {
	return m.distributionModifyTs.Load()
}

func (m *SegmentResourceManager) markDistributionChanged() {
	for {
		previous := m.distributionModifyTs.Load()
		if m.distributionModifyTs.CompareAndSwap(previous, max(previous+1, time.Now().UnixNano())) {
			return
		}
	}
}

// SegmentDistribution reports one actually prepared read per SegmentID. The
// most recently completed, caught-up read wins; requirements and failed attempts
// cannot publish metadata. Older reads/bases keep their original owners, and
// SegmentMetrics continues to account for every retained physical base.
func (m *SegmentResourceManager) SegmentDistribution() []*querypb.SegmentVersionInfo {
	type sample struct {
		resource *segmentResource
		read     *preparedSegmentRead
	}
	var samples []sample
	m.mu.Lock()
	for _, r := range m.resources {
		if r.closing {
			continue
		}
		for i := len(r.reads) - 1; i >= 0; i-- {
			read := r.reads[i]
			if !read.base.caughtUp || read.base.catchupErr != nil {
				continue
			}
			r.operations.Add(1)
			read.base.operationRefs++
			samples = append(samples, sample{r, read})
			break
		}
	}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, sample := range samples {
			sample.read.base.operationRefs--
			sample.resource.wakeLocked()
			sample.resource.operations.Done()
		}
	}()
	result := make([]*querypb.SegmentVersionInfo, 0, len(samples))
	for _, sample := range samples {
		info := proto.Clone(sample.read.distribution).(*querypb.SegmentVersionInfo)
		if readable, ok := UnwrapTransformSegment(sample.read.base.segment).(interface{ QuerySegment() segments.Segment }); ok {
			if s := readable.QuerySegment(); s != nil {
				info.LastDeltaTimestamp = s.LastDeltaTimestamp()
			}
		}
		result = append(result, info)
	}
	return result
}

// Capture the selected artifacts after successful native preparation. A reused
// base's LocalSegment.LoadInfo still describes its initial load, while its private
// read may have reopened a newer manifest or a different index selection.
func preparedReadDistribution(info *querypb.SegmentLoadInfo) *querypb.SegmentVersionInfo {
	indexes := make(map[int64]*querypb.FieldIndexInfo, len(info.GetIndexInfos()))
	for _, index := range info.GetIndexInfos() {
		indexes[index.GetIndexID()] = index
	}
	stats := make(map[int64]*querypb.JsonStatsInfo)
	if paramtable.Get().CommonCfg.EnabledJSONKeyStats.GetAsBool() {
		resolved := packed.NewStatsResolverFromLoadInfo(info).TextAndJSONIndexStatsWithBasePaths()
		jsonStats := resolved.JSONKeyStats
		if resolved.Err() != nil {
			jsonStats = info.GetJsonKeyStatsLogs()
		}
		for fieldID, item := range jsonStats {
			if item.GetJsonKeyStatsDataFormat() == common.JSONStatsDataFormatVersion {
				stats[fieldID] = &querypb.JsonStatsInfo{FieldID: item.GetFieldID(), DataFormatVersion: item.GetJsonKeyStatsDataFormat(), BuildID: item.GetBuildID(), VersionID: item.GetVersion()}
			}
		}
	}
	return proto.Clone(&querypb.SegmentVersionInfo{
		ID: info.GetSegmentID(), Collection: info.GetCollectionID(), Partition: info.GetPartitionID(), Channel: info.GetInsertChannel(),
		Level: info.GetLevel(), IsSorted: info.GetIsSorted(), IndexInfo: indexes, JsonStatsInfo: stats,
		ManifestPath: info.GetManifestPath(), DataVersion: proto.Int32(info.GetDataVersion()),
	}).(*querypb.SegmentVersionInfo)
}
