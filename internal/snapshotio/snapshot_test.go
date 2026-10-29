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
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	assert.NotContains(t, AvroSchemaV3(), "child_fields")
	assert.Contains(t, AvroSchemaV4(), "child_fields")
	assert.NotContains(t, AvroSchemaV4(), "manifest_has_index")
	assert.Contains(t, AvroSchemaV5(), "manifest_has_index")

	currentSchema, err := ManifestSchema()
	require.NoError(t, err)
	assert.NotNil(t, currentSchema)

	schema, err = ManifestSchemaByVersion(SnapshotFormatVersion + 1)
	require.Error(t, err)
	assert.Nil(t, schema)
	assert.Contains(t, err.Error(), "unsupported manifest schema version")
}

func TestParseSnapshotMetadata(t *testing.T) {
	metadata, err := ParseSnapshotMetadata([]byte(`{
		"format_version": 3,
		"snapshot_info": {"id": 10, "name": "snap"},
		"unknown_field": "ignored"
	}`))
	require.NoError(t, err)
	assert.Equal(t, int32(3), metadata.GetFormatVersion())
	assert.Equal(t, int64(10), metadata.GetSnapshotInfo().GetId())

	metadata, err = ParseSnapshotMetadata([]byte(fmt.Sprintf(`{"format_version":%d}`, SnapshotFormatVersion)))
	require.NoError(t, err)
	assert.Equal(t, int32(SnapshotFormatVersion), metadata.GetFormatVersion())

	futureVersion := SnapshotFormatVersion + 1
	_, err = ParseSnapshotMetadata([]byte(fmt.Sprintf(`{"format_version":%d}`, futureVersion)))
	require.NoError(t, err)

	_, err = ParseSnapshotMetadata([]byte(`{`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse metadata JSON")
	assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	assert.Equal(t, int32(1009), merr.Status(err).GetCode())
	assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
	assert.False(t, merr.Status(err).GetRetriable())
	assert.NotNil(t, errors.Unwrap(err), "preserve the original JSON parsing error")

	assert.NoError(t, ValidateFormatVersion(0))
	assert.NoError(t, ValidateFormatVersion(SnapshotFormatVersion))
	assert.NoError(t, ValidateFormatVersion(SnapshotFormatVersion+1))
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
		StartPosition:    &msgpb.MsgPosition{ChannelName: "start", MsgID: []byte{1, 2}, MsgGroup: "g1", Timestamp: 100},
		DmlPosition:      &msgpb.MsgPosition{ChannelName: "dml", MsgID: []byte{3, 4}, MsgGroup: "g2", Timestamp: 200},
		StorageVersion:   2,
		IsSorted:         true,
		CommitTimestamp:  999,
		ManifestHasIndex: proto.Bool(false),
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
	assert.NotNil(t, parsed.ManifestHasIndex)
	assert.False(t, parsed.GetManifestHasIndex())
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
	require.NoError(t, err)

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

func TestParseSegmentManifestV4LeavesManifestHasIndexUnknown(t *testing.T) {
	segment := &datapb.SegmentDescription{
		SegmentId:        1001,
		PartitionId:      2001,
		SegmentLevel:     datapb.SegmentLevel_L1,
		StorageVersion:   3,
		ManifestHasIndex: proto.Bool(true),
	}
	schema, err := ManifestSchemaV4()
	require.NoError(t, err)
	data, err := avro.Marshal(schema, SegmentToManifestEntry(segment))
	require.NoError(t, err)

	parsed, err := ParseSegmentManifest(data, 4)
	require.NoError(t, err)
	assert.Nil(t, parsed.ManifestHasIndex)
}

func TestMarshalSegmentManifestErrors(t *testing.T) {
	mockSchema := mockey.Mock(ManifestSchema).Return(nil, errors.New("schema failed")).Build()
	_, err := MarshalSegmentManifest(&datapb.SegmentDescription{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get manifest schema")
	mockSchema.UnPatch()

	schema, err := ManifestSchema()
	require.NoError(t, err)
	marshalErr := errors.New("marshal failed")
	mockMarshal := mockey.Mock((*ocf.Encoder).Encode).Return(marshalErr).Build()
	defer mockMarshal.UnPatch()
	closeErr := errors.New("close failed")
	mockClose := mockey.Mock((*ocf.Encoder).Close).Return(closeErr).Build()
	defer mockClose.UnPatch()

	data, err := MarshalSegmentManifest(&datapb.SegmentDescription{SegmentId: 100})
	require.ErrorIs(t, err, marshalErr)
	assert.NotErrorIs(t, err, closeErr)
	assert.Nil(t, data)
	assert.Equal(t, 1, mockClose.Times())
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

func TestContainerManifestWriterSchema(t *testing.T) {
	schemaJSON := strings.Replace(AvroSchemaV4(), `{"name": "segment_id", "type": "long"},`, `{"name": "future_field", "type": "string"}, {"name": "segment_id", "type": "long"},`, 1)
	schemaJSON = strings.Replace(schemaJSON, `{"name": "field_id", "type": "long"},`, `{"name": "field_id", "type": "long"}, {"name": "future_binlog_hint", "type": "string"},`, 1)
	schema, err := avro.Parse(schemaJSON)
	require.NoError(t, err)
	base, err := ManifestSchemaV4()
	require.NoError(t, err)
	segment := &datapb.SegmentDescription{SegmentId: 101, PartitionId: 202, CommitTimestamp: 303, Binlogs: []*datapb.FieldBinlog{fieldBinlog(10, 20, "insert")}}
	raw, err := avro.Marshal(base, SegmentToManifestEntry(segment))
	require.NoError(t, err)
	var record map[string]any
	require.NoError(t, avro.Unmarshal(base, raw, &record))
	record["future_field"] = "unknown"
	record["binlog_files"].([]any)[0].(map[string]any)["future_binlog_hint"] = "unknown nested field"
	var buf bytes.Buffer
	encoder, err := ocf.NewEncoderWithSchema(schema, &buf)
	require.NoError(t, err)
	require.NoError(t, encoder.Encode(record))
	require.NoError(t, encoder.Close())
	// Model a legacy reader that has no fields added after V1.
	decoder, err := ocf.NewDecoder(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.True(t, decoder.HasNext())
	var legacy struct {
		SegmentID   int64 `avro:"segment_id"`
		PartitionID int64 `avro:"partition_id"`
		Binlogs     []struct {
			FieldID int64        `avro:"field_id"`
			Binlogs []AvroBinlog `avro:"binlogs"`
		} `avro:"binlog_files"`
	}
	require.NoError(t, decoder.Decode(&legacy))
	assert.Equal(t, int64(101), legacy.SegmentID)
	assert.Equal(t, int64(202), legacy.PartitionID)
	require.Len(t, legacy.Binlogs, 1)
	assert.Equal(t, "insert", legacy.Binlogs[0].Binlogs[0].LogPath)
	restored, err := ParseSegmentManifest(buf.Bytes(), 99)
	require.NoError(t, err)
	assert.Equal(t, segment.GetSegmentId(), restored.GetSegmentId())
	assert.Equal(t, segment.GetCommitTimestamp(), restored.GetCommitTimestamp())
	require.Len(t, restored.GetBinlogs(), 1)
	assert.Equal(t, "insert", restored.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
}

func TestManifestContainerAndLegacy(t *testing.T) {
	segment := &datapb.SegmentDescription{SegmentId: 101}
	for _, version := range []int{0, 1, 2, 3, 4, 5} {
		schema, err := ManifestSchemaByVersion(version)
		require.NoError(t, err)
		data, err := avro.Marshal(schema, SegmentToManifestEntry(segment))
		require.NoError(t, err)
		restored, err := ParseSegmentManifest(data, version)
		require.NoError(t, err)
		assert.Equal(t, int64(101), restored.GetSegmentId())
	}
	data, err := MarshalSegmentManifest(segment)
	require.NoError(t, err)
	assert.True(t, bytes.HasPrefix(data, []byte{'O', 'b', 'j', 1}))
	decoder, err := ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)
	assert.Contains(t, decoder.Schema().String(), "commit_timestamp")
	for _, invalid := range [][]byte{data[:4], data[:len(data)-1]} {
		_, err := ParseSegmentManifest(invalid, SnapshotFormatVersion)
		require.Error(t, err)
	}
	_, err = ParseSegmentManifest([]byte("raw"), SnapshotFormatVersion)
	require.Error(t, err)
}

func TestParseSegmentManifestRejectsOCFForLegacyVersion(t *testing.T) {
	data, err := MarshalSegmentManifest(&datapb.SegmentDescription{SegmentId: 101})
	require.NoError(t, err)

	for _, version := range []int{0, 1, 2, 3, 4, 5} {
		t.Run(fmt.Sprintf("V%d", version), func(t *testing.T) {
			segment, err := ParseSegmentManifest(data, version)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			assert.Nil(t, segment)
			assert.ErrorContains(t, err, fmt.Sprintf("snapshot format %d requires a raw binary manifest", version))
			assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
			assert.False(t, merr.Status(err).GetRetriable())
		})
	}
}

func TestParseSegmentManifestRejectsInvalidBlockSizes(t *testing.T) {
	schema, err := ManifestSchema()
	require.NoError(t, err)
	var buf bytes.Buffer
	encoder, err := ocf.NewEncoderWithSchema(schema, &buf)
	require.NoError(t, err)
	require.NoError(t, encoder.Close())
	header := buf.Bytes()
	valid, err := MarshalSegmentManifest(&datapb.SegmentDescription{SegmentId: 101})
	require.NoError(t, err)
	withBlockHeader := func(prefix []byte, count, size int64) []byte {
		data := binary.AppendVarint(bytes.Clone(prefix), count)
		return binary.AppendVarint(data, size)
	}

	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{"oversized block", withBlockHeader(header, 1, 1<<30), "block size"},
		{"zero-count oversized block", withBlockHeader(header, 0, 1<<30), "block size"},
		{"oversized trailing block", withBlockHeader(valid, 1, 1<<30), "block size"},
		{"negative block size", withBlockHeader(header, 1, -1), "block size"},
		{"truncated header", header[:4], "framing"},
		{"truncated block count", append(bytes.Clone(header), 0x80), "framing"},
		{"truncated block size", binary.AppendVarint(bytes.Clone(header), 1), "framing"},
		{"truncated block data", withBlockHeader(header, 1, 1), "framing"},
		{"truncated block sync", withBlockHeader(header, 1, 0), "framing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A missing guard must not allocate the forged 1 GiB block in this test.
			// Validation must reject the file before creating the OCF decoder.
			patch := mockey.Mock(ocf.NewDecoder).Return(nil, errors.New("unexpected OCF decoder call")).Build()
			defer patch.UnPatch()
			segment, err := ParseSegmentManifest(tc.data, SnapshotFormatVersion)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			assert.Nil(t, segment)
			assert.ErrorContains(t, err, tc.want)
		})
	}
}

func TestParseSegmentManifestContainerCodecs(t *testing.T) {
	schema, err := ManifestSchema()
	require.NoError(t, err)
	for _, codec := range []ocf.CodecName{ocf.Null, ocf.Deflate, ocf.Snappy, ocf.ZStandard} {
		t.Run(string(codec), func(t *testing.T) {
			var buf bytes.Buffer
			encoder, err := ocf.NewEncoderWithSchema(schema, &buf, ocf.WithCodec(codec))
			require.NoError(t, err)
			require.NoError(t, encoder.Encode(SegmentToManifestEntry(&datapb.SegmentDescription{SegmentId: 101})))
			require.NoError(t, encoder.Close())
			segment, err := ParseSegmentManifest(buf.Bytes(), SnapshotFormatVersion)
			require.NoError(t, err)
			assert.Equal(t, int64(101), segment.GetSegmentId())
		})
	}
}

func TestManifestContainerFailures(t *testing.T) {
	schema, err := ManifestSchema()
	require.NoError(t, err)
	for _, count := range []int{0, 2} {
		var buf bytes.Buffer
		encoder, err := ocf.NewEncoderWithSchema(schema, &buf)
		require.NoError(t, err)
		for i := 0; i < count; i++ {
			require.NoError(t, encoder.Encode(SegmentToManifestEntry(&datapb.SegmentDescription{SegmentId: 1})))
		}
		require.NoError(t, encoder.Close())
		_, err = ParseSegmentManifest(buf.Bytes(), SnapshotFormatVersion)
		require.Error(t, err)
	}
	for _, version := range []int{-1} {
		require.Error(t, ValidateFormatVersion(version))
	}
	t.Run("encoder creation", func(t *testing.T) {
		patch := mockey.Mock(ocf.NewEncoderWithSchema).Return(nil, errors.New("header failure")).Build()
		defer patch.UnPatch()
		_, err := MarshalSegmentManifest(&datapb.SegmentDescription{})
		require.ErrorContains(t, err, "failed to create avro container")
	})
	t.Run("encoder close", func(t *testing.T) {
		patch := mockey.Mock((*ocf.Encoder).Close).Return(errors.New("flush failure")).Build()
		defer patch.UnPatch()
		_, err := MarshalSegmentManifest(&datapb.SegmentDescription{})
		require.ErrorContains(t, err, "failed to finalize avro container")
	})
}

func TestParseManifestFailures(t *testing.T) {
	t.Run("legacy schema", func(t *testing.T) {
		patch := mockey.Mock(ManifestSchemaByVersion).Return(nil, errors.New("schema failure")).Build()
		defer patch.UnPatch()
		_, err := ParseSegmentManifest([]byte{0}, 1)
		require.ErrorContains(t, err, "failed to get manifest schema")
	})
	t.Run("legacy data", func(t *testing.T) {
		_, err := ParseSegmentManifest(nil, 1)
		require.ErrorContains(t, err, "failed to parse avro data")
	})
	t.Run("container schema", func(t *testing.T) {
		data, err := avro.Marshal(ocf.HeaderSchema, ocf.Header{
			Magic: [4]byte{'O', 'b', 'j', 1},
			Meta:  map[string][]byte{"avro.schema": []byte("invalid")},
		})
		require.NoError(t, err)
		_, err = ParseSegmentManifest(data, SnapshotFormatVersion)
		require.ErrorContains(t, err, "failed to read avro container header")
	})
	t.Run("container record", func(t *testing.T) {
		var buf bytes.Buffer
		encoder, err := ocf.NewEncoder(`"string"`, &buf)
		require.NoError(t, err)
		require.NoError(t, encoder.Encode("wrong record type"))
		require.NoError(t, encoder.Close())
		_, err = ParseSegmentManifest(buf.Bytes(), SnapshotFormatVersion)
		require.ErrorContains(t, err, "failed to parse avro data")
	})
	t.Run("invalid first block", func(t *testing.T) {
		var buf bytes.Buffer
		schema, err := ManifestSchema()
		require.NoError(t, err)
		encoder, err := ocf.NewEncoderWithSchema(schema, &buf)
		require.NoError(t, err)
		require.NoError(t, encoder.Close())
		data := append(buf.Bytes(), 2, 0)
		data = append(data, make([]byte, 16)...)
		_, err = ParseSegmentManifest(data, SnapshotFormatVersion)
		require.ErrorContains(t, err, "failed to read avro container")
	})
	t.Run("invalid trailing block", func(t *testing.T) {
		data, err := MarshalSegmentManifest(&datapb.SegmentDescription{SegmentId: 1})
		require.NoError(t, err)
		data = append(data, 2, 0)
		data = append(data, make([]byte, 16)...)
		_, err = ParseSegmentManifest(data, SnapshotFormatVersion)
		require.ErrorContains(t, err, "failed to read avro container")
	})
}

func TestFutureSnapshotVersion(t *testing.T) {
	metadata, err := ParseSnapshotMetadata([]byte(`{"format_version":99,"future_optional":"ignored"}`))
	require.NoError(t, err)
	assert.Equal(t, int32(99), metadata.GetFormatVersion())
	data, err := MarshalSegmentManifest(&datapb.SegmentDescription{SegmentId: 123})
	require.NoError(t, err)
	segment, err := ParseSegmentManifest(data, 99)
	require.NoError(t, err)
	assert.Equal(t, int64(123), segment.GetSegmentId())
	_, err = ParseSnapshotMetadata([]byte(`{"format_version":-1}`))
	require.Error(t, err)
	_, err = ParseSegmentManifest(nil, -1)
	require.Error(t, err)
}

func TestLegacyV5MarkerAndFields(t *testing.T) {
	schema, err := ManifestSchemaV5()
	require.NoError(t, err)
	base, err := ManifestSchemaV4()
	require.NoError(t, err)
	segment := &datapb.SegmentDescription{SegmentId: 123, CommitTimestamp: 456, Binlogs: []*datapb.FieldBinlog{fieldBinlog(10, 20, "insert")}}
	raw, err := avro.Marshal(base, SegmentToManifestEntry(segment))
	require.NoError(t, err)
	var record map[string]any
	require.NoError(t, avro.Unmarshal(base, raw, &record))
	for _, marker := range []bool{false, true} {
		record["manifest_has_index"] = marker
		raw, err := avro.Marshal(schema, record)
		require.NoError(t, err)
		restored, err := ParseSegmentManifest(raw, 5)
		require.NoError(t, err)
		require.NotNil(t, restored.ManifestHasIndex)
		assert.Equal(t, marker, restored.GetManifestHasIndex())
		assert.Equal(t, segment.GetSegmentId(), restored.GetSegmentId())
		assert.Equal(t, segment.GetCommitTimestamp(), restored.GetCommitTimestamp())
		require.Len(t, restored.GetBinlogs(), 1)
		assert.Equal(t, "insert", restored.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	}
}

func TestOCFManifestRetainsUpstreamIndexMarker(t *testing.T) {
	for _, marker := range []bool{false, true} {
		data, err := MarshalSegmentManifest(&datapb.SegmentDescription{SegmentId: 123, ManifestHasIndex: proto.Bool(marker)})
		require.NoError(t, err)
		segment, err := ParseSegmentManifest(data, SnapshotFormatVersion)
		require.NoError(t, err)
		require.NotNil(t, segment.ManifestHasIndex)
		assert.Equal(t, marker, segment.GetManifestHasIndex())
	}
}
