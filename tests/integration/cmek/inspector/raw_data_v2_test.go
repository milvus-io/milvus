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

package inspector

import (
	"bytes"
	"testing"

	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/schema"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestLocateRawDataV2EnumeratesEveryAuthoritativeBinlog(t *testing.T) {
	segments := []*datapb.SegmentInfo{
		{
			ID: 31, CollectionID: 11, PartitionID: 21, StorageVersion: 2,
			Binlogs: []*datapb.FieldBinlog{
				{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "raw/a"}, {LogID: 302}}},
				{FieldID: 102, Binlogs: []*datapb.Binlog{{LogPath: "raw/c"}}},
			},
		},
	}

	objects, err := LocateRawDataV2("files", segments)
	require.NoError(t, err)
	require.Equal(t, []RawDataObject{
		{CollectionID: 11, PartitionID: 21, SegmentID: 31, FieldID: 101, Path: "raw/a", StorageVersion: 2},
		{CollectionID: 11, PartitionID: 21, SegmentID: 31, FieldID: 101, Path: "files/insert_log/11/21/31/101/302", StorageVersion: 2},
		{CollectionID: 11, PartitionID: 21, SegmentID: 31, FieldID: 102, Path: "raw/c", StorageVersion: 2},
	}, objects)
}

func TestLocateRawDataV2RejectsWrongVersionAndEmptyPaths(t *testing.T) {
	_, err := LocateRawDataV2("files", []*datapb.SegmentInfo{{ID: 31, StorageVersion: 3}})
	require.ErrorContains(t, err, "storage version 3")

	_, err = LocateRawDataV2("files", []*datapb.SegmentInfo{{
		ID: 31, StorageVersion: 2,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: ""}}}},
	}})
	require.ErrorContains(t, err, "neither a raw-data object path nor a valid log ID")

	_, err = LocateRawDataV2("files", []*datapb.SegmentInfo{
		{ID: 31, CollectionID: 11, StorageVersion: 2, Binlogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "raw/a"}}}}},
		{ID: 32, CollectionID: 12, StorageVersion: 2, Binlogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "raw/b"}}}}},
	})
	require.ErrorContains(t, err, "belongs to collection 12")
}

func TestInspectRawDataV2ValidatesEncryptedParquetEnvelope(t *testing.T) {
	raw := encryptedParquetFixture(t, "17_23_fixture-edek", parquet.AesGcm)
	require.NoError(t, InspectRawDataV2(raw, 17, 23))
}

func TestInspectRawDataV2RejectsWrongIdentityAndCipher(t *testing.T) {
	raw := encryptedParquetFixture(t, "17_23_fixture-edek", parquet.AesGcm)
	require.ErrorContains(t, InspectRawDataV2(raw, 18, 23), "EZ id 17")
	require.ErrorContains(t, InspectRawDataV2(raw, 17, 24), "collection id 23")

	raw = encryptedParquetFixture(t, "17_23_fixture-edek", parquet.AesCtr)
	require.ErrorContains(t, InspectRawDataV2(raw, 17, 23), "AES_GCM_V1")
}

func TestInspectRawDataV2RejectsPlaintextParquet(t *testing.T) {
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt64Node("value", parquet.Repetitions.Required, -1),
	}, -1)
	require.NoError(t, err)
	var sink bytes.Buffer
	writer := file.NewParquetWriter(&sink, root)
	require.NoError(t, writer.Close())

	require.ErrorContains(t, InspectRawDataV2(sink.Bytes(), 17, 23), "encrypted footer")
}

func encryptedParquetFixture(t *testing.T, keyMetadata string, cipher parquet.Cipher) []byte {
	t.Helper()
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt64Node("value", parquet.Repetitions.Required, -1),
	}, -1)
	require.NoError(t, err)
	properties := parquet.NewWriterProperties(parquet.WithEncryptionProperties(
		parquet.NewFileEncryptionProperties("0123456789abcdef", parquet.WithFooterKeyMetadata(keyMetadata), parquet.WithAlg(cipher)),
	))
	var sink bytes.Buffer
	writer := file.NewParquetWriter(&sink, root, file.WithWriterProps(properties))
	require.NoError(t, writer.Close())
	return append([]byte(nil), sink.Bytes()...)
}
