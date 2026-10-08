package qnview

import (
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// CloneQueryViewLoadInfo prevents caller-owned metadata from changing a queued
// load or the configuration pinned by an existing view.
func CloneQueryViewLoadInfo(info QueryViewLoadInfo) QueryViewLoadInfo {
	out := info
	out.PartitionIDs = append([]int64(nil), info.PartitionIDs...)
	out.LoadFields = make([]*messagespb.LoadFieldConfig, len(info.LoadFields))
	for i, field := range info.LoadFields {
		out.LoadFields[i] = proto.Clone(field).(*messagespb.LoadFieldConfig)
	}
	out.IndexInfos = make([]*indexpb.IndexInfo, len(info.IndexInfos))
	for i, index := range info.IndexInfos {
		out.IndexInfos[i] = proto.Clone(index).(*indexpb.IndexInfo)
	}
	out.fieldVersions = make(map[int64]QueryViewLoadInfoVersion, len(info.fieldVersions))
	for id, version := range info.fieldVersions {
		out.fieldVersions[id] = version
	}
	return out
}

// unionLoadInfo preserves all referenced fields. For a field mentioned by more
// than one view, the newest configuration wins (including index removal).
func unionLoadInfo(requests map[qviews.QueryViewKey]segmentLoadRequest) *QueryViewLoadInfo {
	var configs []QueryViewLoadInfo
	for _, request := range requests {
		if request.loadInfo != nil {
			configs = append(configs, *request.loadInfo)
		}
	}
	if len(configs) == 0 {
		return nil
	}
	sort.Slice(configs, func(i, j int) bool { return configs[i].Version < configs[j].Version })
	out := &QueryViewLoadInfo{fieldVersions: make(map[int64]QueryViewLoadInfoVersion)}
	fields := make(map[int64]*messagespb.LoadFieldConfig)
	indexes := make(map[int64]*indexpb.IndexInfo)
	partitions := make(map[int64]struct{})
	for _, config := range configs {
		out.CollectionID, out.Version = config.CollectionID, config.Version
		for _, id := range config.PartitionIDs {
			partitions[id] = struct{}{}
		}
		for _, field := range config.LoadFields {
			id := field.GetFieldId()
			fields[id] = field
			out.fieldVersions[id] = config.Version
			delete(indexes, id)
			for _, index := range config.IndexInfos {
				if index.GetIndexID() == field.GetIndexId() && index.GetFieldID() == id {
					indexes[id] = index
				}
			}
		}
	}
	for id := range partitions {
		out.PartitionIDs = append(out.PartitionIDs, id)
	}
	for _, field := range fields {
		out.LoadFields = append(out.LoadFields, field)
	}
	for _, index := range indexes {
		out.IndexInfos = append(out.IndexInfos, index)
	}
	sort.Slice(out.PartitionIDs, func(i, j int) bool { return out.PartitionIDs[i] < out.PartitionIDs[j] })
	sort.Slice(out.LoadFields, func(i, j int) bool { return out.LoadFields[i].GetFieldId() < out.LoadFields[j].GetFieldId() })
	sort.Slice(out.IndexInfos, func(i, j int) bool { return out.IndexInfos[i].GetIndexID() < out.IndexInfos[j].GetIndexID() })
	cloned := CloneQueryViewLoadInfo(*out)
	return &cloned
}

func sameLoadRequirements(a, b *QueryViewLoadInfo) bool {
	if a == nil || b == nil {
		return a == b
	}
	if len(a.LoadFields) != len(b.LoadFields) || len(a.IndexInfos) != len(b.IndexInfos) {
		return false
	}
	for i := range a.LoadFields {
		if !proto.Equal(a.LoadFields[i], b.LoadFields[i]) {
			return false
		}
	}
	for i := range a.IndexInfos {
		if !proto.Equal(a.IndexInfos[i], b.IndexInfos[i]) {
			return false
		}
	}
	return true
}

func coversLoadRequirements(applied, required *QueryViewLoadInfo) bool {
	if required == nil {
		return true
	}
	if applied == nil {
		return false
	}
	for _, wanted := range required.LoadFields {
		found := false
		for _, loaded := range applied.LoadFields {
			if loaded.GetFieldId() == wanted.GetFieldId() &&
				((proto.Equal(loaded, wanted) && sameFieldIndexConfig(applied, required, wanted.GetIndexId())) || applied.fieldVersions[loaded.GetFieldId()] > required.Version) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// planSegmentSnapshot combines immutable view requirements with physical file
// metadata. Missing requested indexes wait for the metadata stream, rather than
// publishing readiness for a different index.
func planSegmentSnapshot(snapshot SegmentLoadInfoSnapshot, resources *QueryViewLoadInfo) (SegmentLoadInfoSnapshot, bool) {
	if resources == nil || snapshot.LoadInfo == nil {
		return snapshot, true
	}
	planned := snapshot
	planned.resources = resources
	planned.LoadInfo = proto.Clone(snapshot.LoadInfo).(*querypb.SegmentLoadInfo)
	fields := make(map[int64]struct{}, len(resources.LoadFields))
	planned.LoadInfo.IndexInfos = nil
	for _, field := range resources.LoadFields {
		fields[field.GetFieldId()] = struct{}{}
		if field.GetIndexId() == 0 {
			continue
		}
		found := false
		for _, index := range snapshot.LoadInfo.GetIndexInfos() {
			if index.GetFieldID() == field.GetFieldId() && index.GetIndexID() == field.GetIndexId() {
				configured := proto.Clone(index).(*querypb.FieldIndexInfo)
				for _, definition := range resources.IndexInfos {
					if definition.GetIndexID() == index.GetIndexID() {
						configured.IndexParams = mergeIndexLoadParams(configured.IndexParams, definition.GetIndexParams(), definition.GetUserIndexParams())
						break
					}
				}
				planned.LoadInfo.IndexInfos = append(planned.LoadInfo.IndexInfos, configured)
				found = true
				break
			}
		}
		if !found {
			return SegmentLoadInfoSnapshot{}, false
		}
	}
	// Empty fields retain the legacy "all fields" interpretation. Packed
	// column groups are kept whole when any constituent field is referenced.
	if len(fields) > 0 {
		planned.LoadInfo.BinlogPaths = filterLoadBinlogs(planned.LoadInfo.BinlogPaths, fields)
		planned.LoadInfo.Bm25Logs = filterLoadBinlogs(planned.LoadInfo.Bm25Logs, fields)
	}
	planned.IndexInfos = CloneQueryViewLoadInfo(*resources).IndexInfos
	return planned, true
}

func filterLoadBinlogs(logs []*datapb.FieldBinlog, fields map[int64]struct{}) []*datapb.FieldBinlog {
	out := make([]*datapb.FieldBinlog, 0, len(logs))
	for _, log := range logs {
		_, keep := fields[log.GetFieldID()]
		keep = keep || log.GetFieldID() < 100 // RowID and timestamp are always needed.
		for _, id := range log.GetChildFields() {
			_, wanted := fields[id]
			keep = keep || wanted
		}
		if keep {
			out = append(out, log)
		}
	}
	return out
}

func sameFieldIndexConfig(a, b *QueryViewLoadInfo, indexID int64) bool {
	var left, right *indexpb.IndexInfo
	for _, index := range a.IndexInfos {
		if index.GetIndexID() == indexID {
			left = index
			break
		}
	}
	for _, index := range b.IndexInfos {
		if index.GetIndexID() == indexID {
			right = index
			break
		}
	}
	return proto.Equal(left, right)
}

func mergeIndexLoadParams(base []*commonpb.KeyValuePair, overrides ...[]*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	params := make(map[string]string)
	for _, pair := range base {
		params[pair.GetKey()] = pair.GetValue()
	}
	for _, pairs := range overrides {
		for _, pair := range pairs {
			params[pair.GetKey()] = pair.GetValue()
		}
	}
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]*commonpb.KeyValuePair, 0, len(params))
	for _, key := range keys {
		out = append(out, &commonpb.KeyValuePair{Key: key, Value: params[key]})
	}
	return out
}
