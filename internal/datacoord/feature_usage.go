// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// FeatureUsageEntries computes the datacoord-side static groups of the feature
// usage report from in-memory metadata: index_types, metric_types,
// index_params and is_auto_index from index meta; the segment group
// (storage_version, is_sorted, text/json/bm25 stats) from segment meta. It
// reads only local memory and holds no state.
func (s *Server) FeatureUsageEntries() []*internalpb.FeatureEntry {
	if s.meta == nil {
		return nil
	}
	var entries []*internalpb.FeatureEntry
	if s.meta.indexMeta != nil {
		entries = append(entries, featureusage.ComputeIndexEntries(s.meta.indexMeta.ListAllIndexes())...)
	}
	if s.meta.segments != nil {
		segs := s.meta.GetAllSegmentsUnsafe()
		infos := make([]*datapb.SegmentInfo, 0, len(segs))
		for _, seg := range segs {
			if seg != nil && seg.SegmentInfo != nil {
				infos = append(infos, seg.SegmentInfo)
			}
		}
		entries = append(entries, featureusage.ComputeSegmentEntries(infos)...)
	}
	return entries
}

// recordImportFileTypes counts the file types of an accepted import job. It runs
// in DataCoord, where the job is created, not on the DataNode that executes
// it: DataNodes are shared across instances in pooled deployments and must not
// report per-instance usage. Counting at creation also keeps retries from
// counting twice.
func recordImportFileTypes(files []*internalpb.ImportFile) {
	if !featureusage.Enabled() {
		return
	}
	for _, file := range files {
		fileType, err := importutilv2.GetFileType(file)
		if err != nil {
			continue
		}
		if f, ok := featureusage.ImportFileTypeFeature(fileType.String()); ok {
			featureusage.Hit(f)
		}
	}
}

// recordCompactionType counts a compaction task the moment DataCoord persists
// it, for the same reason as recordImportFileTypes.
func recordCompactionType(t datapb.CompactionType) {
	if featureusage.Enabled() {
		featureusage.Hit(featureusage.CompactionTypeFeature(t))
	}
}
