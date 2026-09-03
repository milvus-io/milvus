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
}
