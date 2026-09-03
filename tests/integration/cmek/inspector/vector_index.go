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
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	binlogMagic             = int32(0xfffabc)
	descriptorEventTypeCode = byte(0)
	indexFileEventTypeCode  = byte(7)
	eventHeaderSize         = 17
	descriptorFixedDataSize = 52
	postHeaderLengthsSize   = 8
)

type VectorIndexSet struct {
	CollectionID        int64
	PartitionID         int64
	SegmentID           int64
	FieldID             int64
	IndexID             int64
	BuildID             int64
	CurrentIndexVersion int32
	IndexVersion        int64
	IndexType           string
	MetricType          string
	PathVersion         indexpb.IndexStorePathVersion
	Paths               []string
}

type VectorIndexObject struct {
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	FieldID      int64
	BuildID      int64
	EZID         int64
}

func LocateVectorIndex(ctx context.Context, client types.MixCoordClient, segments []*datapb.SegmentInfo,
	fieldID, indexID int64, expectedIndexType, expectedMetricType string, expectedEngineVersion int32,
	expectedPathVersion indexpb.IndexStorePathVersion,
) ([]VectorIndexSet, error) {
	if len(segments) == 0 {
		return nil, fmt.Errorf("no sealed segments were reported")
	}
	collectionID := segments[0].GetCollectionID()
	segmentIDs := make([]int64, 0, len(segments))
	byID := make(map[int64]*datapb.SegmentInfo, len(segments))
	for _, segment := range segments {
		if segment.GetCollectionID() != collectionID {
			return nil, fmt.Errorf("segment %d belongs to collection %d, want %d", segment.GetID(), segment.GetCollectionID(), collectionID)
		}
		if segment.GetStorageVersion() != storage.StorageV2 {
			return nil, fmt.Errorf("segment %d reported storage version %d, want %d", segment.GetID(), segment.GetStorageVersion(), storage.StorageV2)
		}
		segmentIDs = append(segmentIDs, segment.GetID())
		byID[segment.GetID()] = segment
	}
	response, err := client.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{CollectionID: collectionID, SegmentIDs: segmentIDs})
	if err = merr.CheckRPCCall(response, err); err != nil {
		return nil, err
	}
	sets := make([]VectorIndexSet, 0, len(segments))
	seenPaths := make(map[string]int64)
	for _, segment := range segments {
		segmentInfo := response.GetSegmentInfo()[segment.GetID()]
		if segmentInfo == nil {
			return nil, fmt.Errorf("missing index metadata for segment %d", segment.GetID())
		}
		if segmentInfo.GetCollectionID() != collectionID || segmentInfo.GetSegmentID() != segment.GetID() {
			return nil, fmt.Errorf("index metadata for segment %d reports collection/segment %d/%d, want %d/%d",
				segment.GetID(), segmentInfo.GetCollectionID(), segmentInfo.GetSegmentID(), collectionID, segment.GetID())
		}
		var matches []*indexpb.IndexFilePathInfo
		for _, info := range segmentInfo.GetIndexInfos() {
			if info.GetFieldID() == fieldID && info.GetIndexID() == indexID {
				matches = append(matches, info)
			}
		}
		if len(matches) != 1 {
			return nil, fmt.Errorf("segment %d has %d finished index records for field %d index %d, want exactly one",
				segment.GetID(), len(matches), fieldID, indexID)
		}
		info := matches[0]
		if info.GetSegmentID() != segment.GetID() || info.GetFieldID() != fieldID || info.GetIndexID() != indexID {
			return nil, fmt.Errorf("segment %d has inconsistent vector-index identity", segment.GetID())
		}
		if info.GetBuildID() <= 0 || info.GetIndexVersion() <= 0 {
			return nil, fmt.Errorf("segment %d has invalid build id %d or index generation %d", segment.GetID(), info.GetBuildID(), info.GetIndexVersion())
		}
		if info.GetCurrentIndexVersion() != expectedEngineVersion {
			return nil, fmt.Errorf("segment %d reported vector engine %d, want %d", segment.GetID(), info.GetCurrentIndexVersion(), expectedEngineVersion)
		}
		if info.GetIndexStorePathVersion() != expectedPathVersion {
			return nil, fmt.Errorf("segment %d reported index path version %s, want %s", segment.GetID(), info.GetIndexStorePathVersion(), expectedPathVersion)
		}
		indexType, metricType := "", ""
		for _, parameter := range info.GetIndexParams() {
			switch parameter.GetKey() {
			case common.IndexTypeKey:
				indexType = parameter.GetValue()
			case common.MetricTypeKey:
				metricType = parameter.GetValue()
			}
		}
		if indexType != expectedIndexType {
			return nil, fmt.Errorf("segment %d reported index type %q, want %q", segment.GetID(), indexType, expectedIndexType)
		}
		if metricType != expectedMetricType {
			return nil, fmt.Errorf("segment %d reported metric type %q, want %q", segment.GetID(), metricType, expectedMetricType)
		}
		if len(info.GetIndexFilePaths()) == 0 {
			return nil, fmt.Errorf("segment %d has no vector-index objects", segment.GetID())
		}
		paths := append([]string(nil), info.GetIndexFilePaths()...)
		for _, objectPath := range paths {
			if objectPath == "" {
				return nil, fmt.Errorf("segment %d has an empty vector-index object path", segment.GetID())
			}
			if owner, duplicate := seenPaths[objectPath]; duplicate {
				return nil, fmt.Errorf("vector-index object path %q belongs to both segment %d and segment %d", objectPath, owner, segment.GetID())
			}
			seenPaths[objectPath] = segment.GetID()
		}
		sets = append(sets, VectorIndexSet{
			CollectionID: collectionID, PartitionID: byID[segment.GetID()].GetPartitionID(), SegmentID: segment.GetID(),
			FieldID: fieldID, IndexID: indexID, BuildID: info.GetBuildID(), CurrentIndexVersion: info.GetCurrentIndexVersion(),
			IndexVersion: info.GetIndexVersion(), IndexType: indexType, MetricType: metricType,
			PathVersion: info.GetIndexStorePathVersion(), Paths: paths,
		})
	}
	return sets, nil
}

// InspectIndexDataV2 is a deliberately small test-side parser. It validates
// the cleartext descriptor and verifies that the remaining bytes are not a
// complete plaintext IndexFileEvent. It does not call a production binlog
// reader or ask the fixture cipher plugin to decrypt the payload.
func InspectIndexDataV2(raw []byte, expected VectorIndexObject) error {
	const descriptorPrefixSize = 4 + eventHeaderSize + descriptorFixedDataSize + postHeaderLengthsSize + 4
	if len(raw) < descriptorPrefixSize {
		return fmt.Errorf("V2 IndexData object is too short: %d bytes", len(raw))
	}
	order := binary.LittleEndian
	if int32(order.Uint32(raw[:4])) != binlogMagic {
		return fmt.Errorf("V2 IndexData object has an invalid magic number")
	}
	header := raw[4 : 4+eventHeaderSize]
	if header[8] != descriptorEventTypeCode {
		return fmt.Errorf("V2 IndexData first event type %d is not a descriptor", header[8])
	}
	descriptorLength := int(order.Uint32(header[9:13]))
	nextPosition := int(order.Uint32(header[13:17]))
	if descriptorLength < eventHeaderSize+descriptorFixedDataSize+postHeaderLengthsSize+4 ||
		nextPosition != 4+descriptorLength || nextPosition > len(raw) {
		return fmt.Errorf("V2 IndexData descriptor has invalid length %d or next position %d", descriptorLength, nextPosition)
	}
	data := raw[4+eventHeaderSize:]
	collectionID := int64(order.Uint64(data[0:8]))
	partitionID := int64(order.Uint64(data[8:16]))
	segmentID := int64(order.Uint64(data[16:24]))
	fieldID := int64(order.Uint64(data[24:32]))
	if collectionID != expected.CollectionID || partitionID != expected.PartitionID ||
		segmentID != expected.SegmentID || fieldID != expected.FieldID {
		return fmt.Errorf("V2 IndexData descriptor identity collection=%d partition=%d segment=%d field=%d, want %d/%d/%d/%d",
			collectionID, partitionID, segmentID, fieldID,
			expected.CollectionID, expected.PartitionID, expected.SegmentID, expected.FieldID)
	}
	extraLengthOffset := 4 + eventHeaderSize + descriptorFixedDataSize + postHeaderLengthsSize
	extraLength := int(order.Uint32(raw[extraLengthOffset : extraLengthOffset+4]))
	extraStart := extraLengthOffset + 4
	if extraLength <= 0 || extraStart+extraLength != nextPosition {
		return fmt.Errorf("V2 IndexData descriptor has invalid extras length %d", extraLength)
	}
	var extras struct {
		EZID    json.Number `json:"encryption_zone"`
		BuildID string      `json:"indexBuildID"`
		EDEK    string      `json:"edek"`
	}
	if err := json.Unmarshal(raw[extraStart:nextPosition], &extras); err != nil {
		return fmt.Errorf("parse V2 IndexData descriptor extras: %w", err)
	}
	if extras.EDEK == "" {
		return fmt.Errorf("V2 IndexData envelope has no EDEK")
	}
	ezID, err := extras.EZID.Int64()
	if err != nil || ezID != expected.EZID {
		return fmt.Errorf("V2 IndexData envelope EZ id %s, want %d", extras.EZID, expected.EZID)
	}
	buildID, err := strconv.ParseInt(extras.BuildID, 10, 64)
	if err != nil {
		return fmt.Errorf("V2 IndexData descriptor has invalid build id %q: %w", extras.BuildID, err)
	}
	if buildID != expected.BuildID {
		return fmt.Errorf("V2 IndexData descriptor build id %d, want %d", buildID, expected.BuildID)
	}
	ciphertext := raw[nextPosition:]
	if len(ciphertext) == 0 {
		return fmt.Errorf("V2 IndexData object has no ciphertext")
	}
	if isCompletePlaintextIndexEvent(ciphertext, nextPosition) {
		return fmt.Errorf("V2 IndexData payload is a complete plaintext event")
	}
	return nil
}

func isCompletePlaintextIndexEvent(data []byte, offset int) bool {
	if len(data) < eventHeaderSize+16 || data[8] != indexFileEventTypeCode {
		return false
	}
	eventLength := int(binary.LittleEndian.Uint32(data[9:13]))
	nextPosition := int(binary.LittleEndian.Uint32(data[13:17]))
	return eventLength == len(data) && nextPosition == offset+eventLength
}
