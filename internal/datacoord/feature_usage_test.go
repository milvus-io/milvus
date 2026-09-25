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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func kvPair(k, v string) *commonpb.KeyValuePair { return &commonpb.KeyValuePair{Key: k, Value: v} }

func TestIndexMeta_ListAllIndexes(t *testing.T) {
	m := newSegmentIndexMeta(nil)
	m.indexes[1] = map[UniqueID]*model.Index{
		1: {
			CollectionID: 1, IndexID: 1, FieldID: 101, IndexName: "a",
			IndexParams: []*commonpb.KeyValuePair{kvPair(common.IndexTypeKey, "HNSW"), kvPair(common.MetricTypeKey, "L2")},
		},
		2: {
			CollectionID: 1, IndexID: 2, FieldID: 102, IndexName: "dropped", IsDeleted: true,
			IndexParams: []*commonpb.KeyValuePair{kvPair(common.IndexTypeKey, "DISKANN"), kvPair(common.MetricTypeKey, "IP")},
		},
	}
	m.indexes[2] = map[UniqueID]*model.Index{
		3: {
			CollectionID: 2, IndexID: 3, FieldID: 101, IndexName: "b", IsAutoIndex: true,
			IndexParams: []*commonpb.KeyValuePair{kvPair(common.IndexTypeKey, "IVF_FLAT"), kvPair(common.MetricTypeKey, "IP")},
		},
	}

	got := m.ListAllIndexes()
	require.Len(t, got, 2, "deleted indexes are skipped")

	// Clones: mutating the result does not touch the meta.
	for _, idx := range got {
		idx.IndexName = "mutated"
	}
	assert.Equal(t, "a", m.indexes[1][1].IndexName)
	assert.Equal(t, "b", m.indexes[2][3].IndexName)

	assert.Empty(t, newSegmentIndexMeta(nil).ListAllIndexes())
}

func TestServer_FeatureUsageEntries(t *testing.T) {
	assert.Nil(t, (&Server{}).FeatureUsageEntries())
	assert.Nil(t, (&Server{meta: &meta{}}).FeatureUsageEntries())

	m := newSegmentIndexMeta(nil)
	m.indexes[1] = map[UniqueID]*model.Index{
		1: {CollectionID: 1, IndexID: 1, IndexParams: []*commonpb.KeyValuePair{kvPair(common.IndexTypeKey, "HNSW"), kvPair(common.MetricTypeKey, "L2")}},
	}
	m.indexes[2] = map[UniqueID]*model.Index{
		2: {CollectionID: 2, IndexID: 2, IsAutoIndex: true, IndexParams: []*commonpb.KeyValuePair{kvPair(common.IndexTypeKey, "HNSW"), kvPair(common.MetricTypeKey, "COSINE")}},
	}
	s := &Server{meta: &meta{indexMeta: m}}
	entries := s.FeatureUsageEntries()

	find := func(group, name string) *internalpb.FeatureEntry {
		for _, e := range entries {
			if e.Group == group && e.Name == name {
				return e
			}
		}
		return nil
	}
	require.NotNil(t, find(featureusage.GroupIndexTypes, "HNSW"))
	assert.EqualValues(t, 2, find(featureusage.GroupIndexTypes, "HNSW").Value)
	assert.EqualValues(t, 1, find(featureusage.GroupMetricTypes, "L2").Value)
	assert.EqualValues(t, 1, find(featureusage.GroupMetricTypes, "COSINE").Value)
	assert.EqualValues(t, 1, find(featureusage.GroupDeclared, featureusage.DeclaredIsAutoIndex).Value)
}

func TestServer_FeatureUsageEntries_Segments(t *testing.T) {
	m := &meta{segments: NewSegmentsInfo()}
	m.segments.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, State: commonpb.SegmentState_Flushed, StorageVersion: 2, IsSorted: true}})
	m.segments.SetSegment(2, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, State: commonpb.SegmentState_Dropped, StorageVersion: 7}})
	s := &Server{meta: m}
	entries := s.FeatureUsageEntries()
	find := func(name string) *internalpb.FeatureEntry {
		for _, e := range entries {
			if e.Group == featureusage.GroupSegment && e.Name == name {
				return e
			}
		}
		return nil
	}
	require.NotNil(t, find("storage_version=V2"))
	assert.EqualValues(t, 1, find("storage_version=V2").Value)
	assert.Nil(t, find("storage_version=7"), "dropped segments are skipped")
	assert.EqualValues(t, 1, find(featureusage.SegmentIsSorted).Value)
}

func TestRecordImportFileTypesAndCompactionType(t *testing.T) {
	featureusage.SetEnabled(true)
	snap := func() map[string]int64 {
		out := map[string]int64{}
		for _, e := range featureusage.SnapshotFor(featureusage.RoleMixCoord) {
			out[e.Name] = e.Value
		}
		return out
	}
	before := snap()
	recordImportFileTypes([]*internalpb.ImportFile{
		{Paths: []string{"a/b/rows.parquet"}},
		{Paths: []string{"a/b/rows.json"}},
		{Paths: []string{"a/b/rows.jsonl"}},
		{Paths: []string{"a/b/vec.npy", "a/b/pk.npy"}},
		{Paths: []string{"a/b/rows.csv"}},
		{Paths: []string{"a/b/rows.unknown"}}, // not counted
	})
	recordCompactionType(datapb.CompactionType_ClusteringCompaction)
	recordCompactionType(datapb.CompactionType_MixCompaction)
	recordCompactionType(datapb.CompactionType_UndefinedCompaction)
	after := snap()
	assert.EqualValues(t, 1, after["import_file_type=Parquet"]-before["import_file_type=Parquet"])
	assert.EqualValues(t, 1, after["import_file_type=JSON"]-before["import_file_type=JSON"])
	assert.EqualValues(t, 1, after["import_file_type=JSONLine"]-before["import_file_type=JSONLine"])
	assert.EqualValues(t, 1, after["import_file_type=NumPy"]-before["import_file_type=NumPy"])
	assert.EqualValues(t, 1, after["import_file_type=CSV"]-before["import_file_type=CSV"])
	assert.EqualValues(t, 1, after["compaction=clustering"]-before["compaction=clustering"])
	assert.EqualValues(t, 1, after["compaction=mix"]-before["compaction=mix"])
	assert.EqualValues(t, 1, after["compaction=_other"]-before["compaction=_other"])
	for name := range after {
		assert.Regexp(t, `^(import_file_type|compaction)=`, name, "MixCoord role holds only the DataCoord-side counters")
	}
}

// TestRecordImportFileTypesCountsTypesNotFiles pins the unit of the import
// counters: one accepted import request carrying many files of one type is one
// use of that type, the same way recordCompactionType counts one per task.
// Counting per file would report a job with a thousand Parquet files as a
// thousand uses of Parquet.
func TestRecordImportFileTypesCountsTypesNotFiles(t *testing.T) {
	featureusage.SetEnabled(true)
	snap := func() map[string]int64 {
		out := map[string]int64{}
		for _, e := range featureusage.SnapshotFor(featureusage.RoleMixCoord) {
			out[e.Name] = e.Value
		}
		return out
	}
	files := make([]*internalpb.ImportFile, 0, 1001)
	for i := 0; i < 1000; i++ {
		files = append(files, &internalpb.ImportFile{Paths: []string{fmt.Sprintf("a/b/part-%04d.parquet", i)}})
	}
	files = append(files, &internalpb.ImportFile{Paths: []string{"a/b/extra.csv"}})

	before := snap()
	recordImportFileTypes(files)
	after := snap()

	assert.EqualValues(t, 1, after["import_file_type=Parquet"]-before["import_file_type=Parquet"])
	assert.EqualValues(t, 1, after["import_file_type=CSV"]-before["import_file_type=CSV"])
}
