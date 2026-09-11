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
	"encoding/binary"
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

func TestLocateRawDataV2RejectsIncompleteObjectSets(t *testing.T) {
	for _, test := range []struct {
		name     string
		segments []*datapb.SegmentInfo
		want     string
	}{
		{name: "no segments", want: "no sealed segments"},
		{name: "no fields", segments: []*datapb.SegmentInfo{{ID: 31, StorageVersion: 2}}, want: "no raw-data FieldBinlog"},
		{name: "no objects", segments: []*datapb.SegmentInfo{{
			ID: 31, StorageVersion: 2,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 101}},
		}}, want: "no raw-data Binlog"},
		{name: "duplicate objects", segments: []*datapb.SegmentInfo{{
			ID: 31, StorageVersion: 2,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "raw/a"}, {LogPath: "raw/a"}}}},
		}}, want: "reported more than once"},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects, err := LocateRawDataV2("files", test.segments)
			require.ErrorContains(t, err, test.want)
			require.Nil(t, objects)
		})
	}
}

func TestInspectRawDataV2RejectsMalformedKeyMetadata(t *testing.T) {
	for _, test := range []struct {
		name     string
		metadata string
		want     string
	}{
		{name: "missing parts", metadata: "17_23", want: "must be <ezID>_<collectionID>_<EDEK>"},
		{name: "empty EDEK", metadata: "17_23_", want: "must be <ezID>_<collectionID>_<EDEK>"},
		{name: "invalid EZ", metadata: "bad_23_edek", want: "invalid EZ id"},
		{name: "invalid collection", metadata: "17_bad_edek", want: "invalid collection id"},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := encryptedParquetFixture(t, test.metadata, parquet.AesGcm)
			require.ErrorContains(t, InspectRawDataV2(raw, 17, 23), test.want)
		})
	}
}

func TestInspectRawDataV2RejectsDamagedFooter(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func([]byte) []byte
		want   string
	}{
		{name: "truncated header", mutate: func(raw []byte) []byte { return raw[:4] }, want: "encrypted footer"},
		{name: "zero footer size", mutate: func(raw []byte) []byte {
			binary.LittleEndian.PutUint32(raw[len(raw)-8:], 0)
			return raw
		}, want: "invalid size"},
		{name: "footer extends before header", mutate: func(raw []byte) []byte {
			binary.LittleEndian.PutUint32(raw[len(raw)-8:], uint32(len(raw)))
			return raw
		}, want: "invalid size"},
		{name: "truncated crypto metadata", mutate: func(raw []byte) []byte {
			// A compact-protocol field header without its required value.
			raw = append(append([]byte(nil), raw[:4]...), 0x18, 1, 0, 0, 0, 'P', 'A', 'R', 'E')
			return raw
		}, want: "parse Storage V2 encrypted Parquet crypto metadata"},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := encryptedParquetFixture(t, "17_23_edek", parquet.AesGcm)
			require.ErrorContains(t, InspectRawDataV2(test.mutate(raw), 17, 23), test.want)
		})
	}
}
