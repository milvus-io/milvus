// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapshotio

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestManifestSchemaByVersion(t *testing.T) {
	schema, err := ManifestSchema()
	require.NoError(t, err)
	require.NotNil(t, schema)

	legacySchema, err := ManifestSchemaByVersion(1)
	require.NoError(t, err)
	require.NotNil(t, legacySchema)
	assert.NotContains(t, AvroSchemaV1(), "index_store_path_version")
	assert.NotContains(t, AvroSchemaV2(), "commit_timestamp")
	assert.Contains(t, AvroSchemaV3(), "commit_timestamp")

	currentSchema, err := ManifestSchemaByVersion(SnapshotFormatVersion)
	require.NoError(t, err)
	assert.NotNil(t, currentSchema)

	schema, err = ManifestSchemaByVersion(SnapshotFormatVersion + 1)
	require.Error(t, err)
	assert.Nil(t, schema)
	assert.Contains(t, err.Error(), "unsupported manifest schema version")
}

func TestParseSnapshotMetadataWithVersionCheck(t *testing.T) {
	metadata, err := ParseSnapshotMetadataWithVersionCheck([]byte(`{
		"format_version": 3,
		"snapshot_info": {"id": 10, "name": "snap"},
		"unknown_field": "ignored"
	}`))
	require.NoError(t, err)
	assert.Equal(t, int32(SnapshotFormatVersion), metadata.GetFormatVersion())
	assert.Equal(t, int64(10), metadata.GetSnapshotInfo().GetId())

	_, err = ParseSnapshotMetadataWithVersionCheck([]byte(`{"format_version":99}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "incompatible snapshot format")

	_, err = ParseSnapshotMetadataWithVersionCheck([]byte(`{`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse metadata JSON")

	assert.NoError(t, ValidateFormatVersion(0))
	assert.NoError(t, ValidateFormatVersion(SnapshotFormatVersion))
	assert.Error(t, ValidateFormatVersion(SnapshotFormatVersion+1))
}

func TestSegmentManifestRoundTrip(t *testing.T) {
	segment := &datapb.SegmentDescription{
		SegmentId:    1001,
		PartitionId:  2001,
		SegmentLevel: datapb.SegmentLevel_L1,
		ChannelName:  "by-dev-rootcoord-dml_0",
		NumOfRows:    3001,
		Binlogs: []*datapb.FieldBinlog{
			fieldBinlog(101, 1, "insert-log"),
		},
		Deltalogs: []*datapb.FieldBinlog{
			fieldBinlog(102, 2, "delta-log"),
		},
		Statslogs: []*datapb.FieldBinlog{
			fieldBinlog(103, 3, "stats-log"),
		},
		Bm25Statslogs: []*datapb.FieldBinlog{
			fieldBinlog(104, 4, "bm25-log"),
		},
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				SegmentID:                 1001,
				FieldID:                   101,
				IndexID:                   201,
				BuildID:                   301,
				IndexName:                 "vec_idx",
				IndexParams:               []*commonpb.KeyValuePair{{Key: "metric_type", Value: "L2"}},
				IndexFilePaths:            []string{"indexes/301/1"},
				SerializedSize:            4096,
				IndexVersion:              5,
				NumRows:                   3001,
				CurrentIndexVersion:       7,
				CurrentScalarIndexVersion: 8,
				MemSize:                   8192,
				IndexStorePathVersion:     indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
			},
		},
		TextIndexFiles: map[int64]*datapb.TextIndexStats{
			101: {
				FieldID:                   101,
				Version:                   11,
				Files:                     []string{"text/1"},
				LogSize:                   12,
				MemorySize:                13,
				BuildID:                   14,
				CurrentScalarIndexVersion: 15,
			},
		},
		JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
			102: {
				FieldID:                102,
				Version:                21,
				Files:                  []string{"json/1"},
				LogSize:                22,
				MemorySize:             23,
				BuildID:                24,
				JsonKeyStatsDataFormat: 25,
			},
		},
		StartPosition:   &msgpb.MsgPosition{ChannelName: "start", MsgID: []byte{1, 2}, MsgGroup: "g1", Timestamp: 100},
		DmlPosition:     &msgpb.MsgPosition{ChannelName: "dml", MsgID: []byte{3, 4}, MsgGroup: "g2", Timestamp: 200},
		StorageVersion:  2,
		IsSorted:        true,
		CommitTimestamp: 999,
	}

	entry := SegmentToManifestEntry(segment)
	restored := ManifestEntryToSegment(entry)
	assert.True(t, proto.Equal(segment, restored))

	data, err := MarshalSegmentManifest(segment)
	require.NoError(t, err)
	parsed, err := ParseSegmentManifest(data, SnapshotFormatVersion)
	require.NoError(t, err)
	assert.Equal(t, segment.GetSegmentId(), parsed.GetSegmentId())
	assert.Equal(t, segment.GetPartitionId(), parsed.GetPartitionId())
	assert.Equal(t, segment.GetSegmentLevel(), parsed.GetSegmentLevel())
	assert.Equal(t, segment.GetChannelName(), parsed.GetChannelName())
	assert.Equal(t, segment.GetNumOfRows(), parsed.GetNumOfRows())
	assert.Equal(t, segment.GetStorageVersion(), parsed.GetStorageVersion())
	assert.Equal(t, segment.GetIsSorted(), parsed.GetIsSorted())
	assert.Equal(t, segment.GetCommitTimestamp(), parsed.GetCommitTimestamp())
	require.Len(t, parsed.GetBinlogs(), 1)
	assert.Equal(t, segment.GetBinlogs()[0].GetBinlogs()[0].GetLogPath(), parsed.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	require.Len(t, parsed.GetDeltalogs(), 1)
	assert.Equal(t, segment.GetDeltalogs()[0].GetBinlogs()[0].GetLogID(), parsed.GetDeltalogs()[0].GetBinlogs()[0].GetLogID())
	require.Len(t, parsed.GetIndexFiles(), 1)
	assert.Equal(t, segment.GetIndexFiles()[0].GetIndexStorePathVersion(), parsed.GetIndexFiles()[0].GetIndexStorePathVersion())
	assert.Equal(t, segment.GetStartPosition().GetTimestamp(), parsed.GetStartPosition().GetTimestamp())
	assert.Equal(t, segment.GetDmlPosition().GetMsgGroup(), parsed.GetDmlPosition().GetMsgGroup())
	assert.Equal(t, segment.GetTextIndexFiles()[101].GetFiles(), parsed.GetTextIndexFiles()[101].GetFiles())
	assert.Equal(t, segment.GetJsonKeyIndexFiles()[102].GetJsonKeyStatsDataFormat(), parsed.GetJsonKeyIndexFiles()[102].GetJsonKeyStatsDataFormat())

	_, err = ParseSegmentManifest(data, SnapshotFormatVersion+1)
	require.Error(t, err)

	_, err = ParseSegmentManifest([]byte("not-avro"), SnapshotFormatVersion)
	require.Error(t, err)
}

func TestParseSegmentManifestV2DefaultsCommitTimestamp(t *testing.T) {
	segment := &datapb.SegmentDescription{
		SegmentId:      1001,
		PartitionId:    2001,
		SegmentLevel:   datapb.SegmentLevel_L1,
		StorageVersion: 2,
		IsSorted:       true,
	}
	schema, err := ManifestSchemaV2()
	require.NoError(t, err)
	data, err := avro.Marshal(schema, SegmentToManifestEntry(segment))
	require.NoError(t, err)

	parsed, err := ParseSegmentManifest(data, 2)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), parsed.GetCommitTimestamp())
}

func TestMarshalSegmentManifestErrors(t *testing.T) {
	mockSchema := mockey.Mock(ManifestSchema).Return(nil, errors.New("schema failed")).Build()
	_, err := MarshalSegmentManifest(&datapb.SegmentDescription{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get manifest schema")
	mockSchema.UnPatch()

	schema, err := ManifestSchema()
	require.NoError(t, err)
	mockMarshal := mockey.Mock(avro.Marshal).Return(nil, errors.New("marshal failed")).Build()
	defer mockMarshal.UnPatch()

	_, err = MarshalSegmentManifest(&datapb.SegmentDescription{SegmentId: 100})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to serialize entry to avro")
	_ = schema
}

func TestOptionalManifestConversions(t *testing.T) {
	assert.Nil(t, AvroToMsgPosition(nil))
	assert.Equal(t, &AvroMsgPosition{MsgID: []byte{}}, MsgPositionToAvro(nil))
	assert.Nil(t, TextIndexStatsToAvro(nil))
	assert.Nil(t, AvroToTextIndexStats(nil))
	assert.Nil(t, JSONKeyStatsToAvro(nil))
	assert.Nil(t, AvroToJSONKeyStats(nil))
	assert.Empty(t, TextIndexMapToAvro(nil))
	assert.Empty(t, AvroToTextIndexMap(nil))
	assert.Empty(t, JSONKeyIndexMapToAvro(nil))
	assert.Empty(t, AvroToJSONKeyIndexMap(nil))
}

func fieldBinlog(fieldID int64, logID int64, logPath string) *datapb.FieldBinlog {
	return &datapb.FieldBinlog{
		FieldID: fieldID,
		Binlogs: []*datapb.Binlog{
			{
				EntriesNum:    10,
				TimestampFrom: 100,
				TimestampTo:   200,
				LogPath:       logPath,
				LogSize:       300,
				LogID:         logID,
				MemorySize:    400,
			},
		},
	}
}
