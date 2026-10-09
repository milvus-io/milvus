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
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLocateVectorIndexReturnsCompleteSegmentSets(t *testing.T) {
	client := mocks.NewMockMixCoordClient(t)
	client.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(&indexpb.GetIndexInfoResponse{
		Status: merr.Success(),
		SegmentInfo: map[int64]*indexpb.SegmentInfo{
			31: {CollectionID: 11, SegmentID: 31, IndexInfos: []*indexpb.IndexFilePathInfo{{
				SegmentID: 31, FieldID: 101, IndexID: 51, BuildID: 61, IndexName: "vector_idx",
				IndexParams:    []*commonpb.KeyValuePair{{Key: "index_type", Value: "HNSW"}, {Key: "metric_type", Value: "L2"}},
				IndexFilePaths: []string{"index/a", "index/b"}, IndexVersion: 3, CurrentIndexVersion: 8,
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
			}}},
		},
	}, nil)

	sets, err := LocateVectorIndex(context.Background(), client, []*datapb.SegmentInfo{{
		ID: 31, CollectionID: 11, PartitionID: 21, StorageVersion: 2,
	}}, 101, 51, "HNSW", "L2", 8, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED)
	require.NoError(t, err)
	require.Equal(t, []VectorIndexSet{{
		CollectionID: 11, PartitionID: 21, SegmentID: 31, FieldID: 101, IndexID: 51, BuildID: 61,
		CurrentIndexVersion: 8, IndexVersion: 3, IndexType: "HNSW", MetricType: "L2",
		PathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
		Paths:       []string{"index/a", "index/b"},
	}}, sets)
}

func TestLocateVectorIndexRejectsWrongIdentity(t *testing.T) {
	client := mocks.NewMockMixCoordClient(t)
	client.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(&indexpb.GetIndexInfoResponse{
		Status: merr.Success(),
		SegmentInfo: map[int64]*indexpb.SegmentInfo{
			31: {CollectionID: 11, SegmentID: 31, IndexInfos: []*indexpb.IndexFilePathInfo{{
				SegmentID: 31, FieldID: 101, IndexID: 51, BuildID: 61,
				IndexParams:    []*commonpb.KeyValuePair{{Key: "index_type", Value: "IVF_FLAT"}, {Key: "metric_type", Value: "L2"}},
				IndexFilePaths: []string{"index/shared"}, IndexVersion: 3, CurrentIndexVersion: 8,
			}}},
		},
	}, nil)

	_, err := LocateVectorIndex(context.Background(), client, []*datapb.SegmentInfo{{
		ID: 31, CollectionID: 11, PartitionID: 21, StorageVersion: 2,
	}}, 101, 51, "HNSW", "L2", 8, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED)
	require.ErrorContains(t, err, "index type")
}

func TestLocateVectorIndexRejectsDuplicatePaths(t *testing.T) {
	client := mocks.NewMockMixCoordClient(t)
	client.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(&indexpb.GetIndexInfoResponse{
		Status: merr.Success(),
		SegmentInfo: map[int64]*indexpb.SegmentInfo{
			31: vectorIndexSegmentInfo(11, 31, "index/shared"),
			32: vectorIndexSegmentInfo(11, 32, "index/shared"),
		},
	}, nil)

	_, err := LocateVectorIndex(context.Background(), client, []*datapb.SegmentInfo{
		{ID: 31, CollectionID: 11, PartitionID: 21, StorageVersion: 2},
		{ID: 32, CollectionID: 11, PartitionID: 21, StorageVersion: 2},
	}, 101, 51, "HNSW", "L2", 8, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED)
	require.ErrorContains(t, err, "belongs to both")
}

func vectorIndexSegmentInfo(collectionID, segmentID int64, path string) *indexpb.SegmentInfo {
	return &indexpb.SegmentInfo{CollectionID: collectionID, SegmentID: segmentID, IndexInfos: []*indexpb.IndexFilePathInfo{{
		SegmentID: segmentID, FieldID: 101, IndexID: 51, BuildID: segmentID + 30,
		IndexParams:    []*commonpb.KeyValuePair{{Key: "index_type", Value: "HNSW"}, {Key: "metric_type", Value: "L2"}},
		IndexFilePaths: []string{path}, IndexVersion: 3, CurrentIndexVersion: 8,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
	}}}
}

func TestLocateVectorIndexRejectsIncompleteMetadata(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*indexpb.SegmentInfo)
		want   string
	}{
		{name: "wrong collection", mutate: func(info *indexpb.SegmentInfo) { info.CollectionID++ }, want: "reports collection/segment"},
		{name: "wrong segment", mutate: func(info *indexpb.SegmentInfo) { info.SegmentID++ }, want: "reports collection/segment"},
		{name: "no finished record", mutate: func(info *indexpb.SegmentInfo) { info.IndexInfos = nil }, want: "0 finished index records"},
		{name: "duplicate finished record", mutate: func(info *indexpb.SegmentInfo) {
			info.IndexInfos = append(info.IndexInfos, info.IndexInfos[0])
		}, want: "2 finished index records"},
		{name: "wrong field", mutate: func(info *indexpb.SegmentInfo) { info.IndexInfos[0].FieldID++ }, want: "0 finished index records"},
		{name: "wrong index", mutate: func(info *indexpb.SegmentInfo) { info.IndexInfos[0].IndexID++ }, want: "0 finished index records"},
		{name: "wrong record segment", mutate: func(info *indexpb.SegmentInfo) { info.IndexInfos[0].SegmentID++ }, want: "inconsistent vector-index identity"},
		{name: "missing build ID", mutate: func(info *indexpb.SegmentInfo) { info.IndexInfos[0].BuildID = 0 }, want: "invalid build id"},
		{name: "missing generation", mutate: func(info *indexpb.SegmentInfo) { info.IndexInfos[0].IndexVersion = 0 }, want: "index generation 0"},
		{name: "wrong engine", mutate: func(info *indexpb.SegmentInfo) { info.IndexInfos[0].CurrentIndexVersion++ }, want: "vector engine"},
		{name: "wrong path version", mutate: func(info *indexpb.SegmentInfo) {
			info.IndexInfos[0].IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED
		}, want: "index path version"},
		{name: "wrong metric", mutate: func(info *indexpb.SegmentInfo) { info.IndexInfos[0].IndexParams[1].Value = "IP" }, want: "metric type"},
		{name: "no objects", mutate: func(info *indexpb.SegmentInfo) { info.IndexInfos[0].IndexFilePaths = nil }, want: "no vector-index objects"},
		{name: "empty path", mutate: func(info *indexpb.SegmentInfo) { info.IndexInfos[0].IndexFilePaths = []string{""} }, want: "empty vector-index object path"},
	} {
		t.Run(test.name, func(t *testing.T) {
			info := vectorIndexSegmentInfo(11, 31, "index/a")
			test.mutate(info)
			client := mocks.NewMockMixCoordClient(t)
			client.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(&indexpb.GetIndexInfoResponse{
				Status: merr.Success(), SegmentInfo: map[int64]*indexpb.SegmentInfo{31: info},
			}, nil)
			sets, err := LocateVectorIndex(context.Background(), client, []*datapb.SegmentInfo{{
				ID: 31, CollectionID: 11, StorageVersion: 2,
			}}, 101, 51, "HNSW", "L2", 8, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED)
			require.ErrorContains(t, err, test.want)
			require.Nil(t, sets)
		})
	}
}

func TestLocateVectorIndexRejectsMissingMetadata(t *testing.T) {
	for _, test := range []struct {
		name     string
		response *indexpb.GetIndexInfoResponse
		err      error
		want     string
	}{
		{name: "RPC error", err: merr.ErrServiceNotReady, want: "service not ready"},
		{name: "status error", response: &indexpb.GetIndexInfoResponse{Status: merr.Status(merr.ErrServiceNotReady)}, want: "service not ready"},
		{name: "missing segment", response: &indexpb.GetIndexInfoResponse{Status: merr.Success()}, want: "missing index metadata"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := mocks.NewMockMixCoordClient(t)
			client.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).Return(test.response, test.err)
			sets, err := LocateVectorIndex(context.Background(), client, []*datapb.SegmentInfo{{
				ID: 31, CollectionID: 11, StorageVersion: 2,
			}}, 101, 51, "HNSW", "L2", 8, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED)
			require.ErrorContains(t, err, test.want)
			require.Nil(t, sets)
		})
	}
}

type reversingEncryptor struct{}

func (reversingEncryptor) Encrypt(plainText []byte) ([]byte, error) {
	cipherText := append([]byte(nil), plainText...)
	for left, right := 0, len(cipherText)-1; left < right; left, right = left+1, right-1 {
		cipherText[left], cipherText[right] = cipherText[right], cipherText[left]
	}
	return cipherText, nil
}

type identityEncryptor struct{}

func (identityEncryptor) Encrypt(plainText []byte) ([]byte, error) {
	return append([]byte(nil), plainText...), nil
}

func TestInspectIndexDataV2ValidatesDescriptorAndCiphertext(t *testing.T) {
	const (
		ezID         int64 = 17
		collectionID int64 = 11
		partitionID  int64 = 21
		segmentID    int64 = 31
		fieldID      int64 = 101
		buildID      int64 = 61
	)
	writer := storage.NewInsertBinlogWriter(schemapb.DataType_Int8, collectionID, partitionID, segmentID, fieldID, false,
		storage.WithWriterEncryptionContext(ezID, []byte("fixture-edek"), reversingEncryptor{}))
	writer.AddExtra("indexBuildID", "61")
	writer.AddExtra("original_size", "4")
	event, err := writer.NextInsertEventWriter()
	require.NoError(t, err)
	require.NoError(t, event.AddByteToPayload([]byte{1, 2, 3, 4}, nil))
	event.SetEventTimestamp(1, 2)
	writer.SetEventTimeStamp(1, 2)
	require.NoError(t, writer.Finish())
	raw, err := writer.GetBuffer()
	require.NoError(t, err)

	require.NoError(t, InspectIndexDataV2(raw, VectorIndexObject{
		CollectionID: collectionID, PartitionID: partitionID, SegmentID: segmentID,
		FieldID: fieldID, BuildID: buildID, EZID: ezID,
	}))
	require.ErrorContains(t, InspectIndexDataV2(raw, VectorIndexObject{
		CollectionID: collectionID, PartitionID: partitionID, SegmentID: segmentID,
		FieldID: fieldID, BuildID: buildID + 1, EZID: ezID,
	}), "build id 61")

	plaintextWriter := storage.NewInsertBinlogWriter(schemapb.DataType_Int8, collectionID, partitionID, segmentID, fieldID, false,
		storage.WithWriterEncryptionContext(ezID, []byte("fixture-edek"), identityEncryptor{}))
	plaintextWriter.AddExtra("indexBuildID", "61")
	plaintextWriter.AddExtra("original_size", "4")
	plaintextEvent, err := plaintextWriter.NextInsertEventWriter()
	require.NoError(t, err)
	require.NoError(t, plaintextEvent.AddByteToPayload([]byte{1, 2, 3, 4}, nil))
	plaintextEvent.SetEventTimestamp(1, 2)
	plaintextWriter.SetEventTimeStamp(1, 2)
	require.NoError(t, plaintextWriter.Finish())
	plaintext, err := plaintextWriter.GetBuffer()
	require.NoError(t, err)
	descriptorNext := int(binary.LittleEndian.Uint32(plaintext[17:21]))
	plaintext[descriptorNext+8] = indexFileEventTypeCode
	require.ErrorContains(t, InspectIndexDataV2(plaintext, VectorIndexObject{
		CollectionID: collectionID, PartitionID: partitionID, SegmentID: segmentID,
		FieldID: fieldID, BuildID: buildID, EZID: ezID,
	}), "plaintext event")
	for _, suffix := range [][]byte{{0}, []byte("authentication-tag")} {
		plaintextWithSuffix := append(append([]byte(nil), plaintext...), suffix...)
		require.ErrorContains(t, InspectIndexDataV2(plaintextWithSuffix, VectorIndexObject{
			CollectionID: collectionID, PartitionID: partitionID, SegmentID: segmentID,
			FieldID: fieldID, BuildID: buildID, EZID: ezID,
		}), "plaintext event")
	}
}

func TestInspectIndexDataV2RejectsDamagedEnvelope(t *testing.T) {
	expected := VectorIndexObject{CollectionID: 11, PartitionID: 21, SegmentID: 31, FieldID: 101, BuildID: 61, EZID: 17}
	writer := storage.NewInsertBinlogWriter(schemapb.DataType_Int8, 11, 21, 31, 101, false,
		storage.WithWriterEncryptionContext(17, []byte("fixture-edek"), reversingEncryptor{}))
	defer writer.Close()
	writer.AddExtra("indexBuildID", "61")
	writer.AddExtra("original_size", "4")
	event, err := writer.NextInsertEventWriter()
	require.NoError(t, err)
	require.NoError(t, event.AddByteToPayload([]byte{1, 2, 3, 4}, nil))
	event.SetEventTimestamp(1, 2)
	writer.SetEventTimeStamp(1, 2)
	require.NoError(t, writer.Finish())
	valid, err := writer.GetBuffer()
	require.NoError(t, err)
	require.NoError(t, InspectIndexDataV2(valid, expected))
	nextPosition := int(binary.LittleEndian.Uint32(valid[17:21]))
	const extraLengthOffset = 4 + eventHeaderSize + descriptorFixedDataSize + postHeaderLengthsSize

	for _, test := range []struct {
		name   string
		mutate func([]byte) []byte
		want   string
	}{
		{name: "truncated descriptor", mutate: func(raw []byte) []byte { return raw[:4] }, want: "too short"},
		{name: "wrong magic", mutate: func(raw []byte) []byte { raw[0]++; return raw }, want: "invalid magic"},
		{name: "wrong first event", mutate: func(raw []byte) []byte { raw[12]++; return raw }, want: "not a descriptor"},
		{name: "descriptor length mismatch", mutate: func(raw []byte) []byte { raw[13]++; return raw }, want: "invalid length"},
		{name: "descriptor points past EOF", mutate: func(raw []byte) []byte { return raw[:nextPosition-1] }, want: "invalid length"},
		{name: "wrong collection", mutate: func(raw []byte) []byte { raw[4+eventHeaderSize]++; return raw }, want: "descriptor identity"},
		{name: "extras length mismatch", mutate: func(raw []byte) []byte { raw[extraLengthOffset]++; return raw }, want: "invalid extras length"},
		{name: "invalid extras JSON", mutate: func(raw []byte) []byte { raw[extraLengthOffset+4] = '!'; return raw }, want: "parse V2 IndexData descriptor extras"},
		{name: "no ciphertext", mutate: func(raw []byte) []byte { return raw[:nextPosition] }, want: "no ciphertext"},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := test.mutate(append([]byte(nil), valid...))
			require.ErrorContains(t, InspectIndexDataV2(raw, expected), test.want)
		})
	}

	for _, test := range []struct {
		name  string
		key   string
		value interface{}
		want  string
	}{
		{name: "empty EDEK", key: "edek", value: "", want: "no EDEK"},
		{name: "wrong EZ", key: "encryption_zone", value: 18, want: "EZ id 18"},
		{name: "fractional EZ", key: "encryption_zone", value: 17.5, want: "EZ id 17.5"},
		{name: "invalid build ID", key: "indexBuildID", value: "bad", want: "invalid build id"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var extras map[string]interface{}
			require.NoError(t, json.Unmarshal(valid[extraLengthOffset+4:nextPosition], &extras))
			extras[test.key] = test.value
			encoded, err := json.Marshal(extras)
			require.NoError(t, err)
			raw := append(append([]byte(nil), valid[:extraLengthOffset+4]...), encoded...)
			binary.LittleEndian.PutUint32(raw[13:17], uint32(len(raw)-4))
			binary.LittleEndian.PutUint32(raw[17:21], uint32(len(raw)))
			binary.LittleEndian.PutUint32(raw[extraLengthOffset:], uint32(len(encoded)))
			raw = append(raw, valid[nextPosition:]...)
			require.ErrorContains(t, InspectIndexDataV2(raw, expected), test.want)
		})
	}
}
