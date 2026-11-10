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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
)

const (
	binlogMagic             = int32(0xfffabc)
	descriptorEventTypeCode = byte(0)
	indexFileEventTypeCode  = byte(7)
	eventHeaderSize         = 17
	descriptorFixedDataSize = 52
	postHeaderLengthsSize   = 8
)

type IndexDataV2Identity struct {
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	FieldID      int64
	BuildID      int64
	EZID         int64
}

// InspectIndexDataV2 is a deliberately small test-side parser. It validates
// the cleartext descriptor and verifies that the remaining bytes do not begin
// with a complete plaintext IndexFileEvent. It does not call a production
// binlog reader or ask the fixture cipher plugin to decrypt the payload.
func InspectIndexDataV2(raw []byte, expected IndexDataV2Identity) error {
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
		return fmt.Errorf("V2 IndexData payload contains a complete plaintext event prefix")
	}
	return nil
}

func isCompletePlaintextIndexEvent(data []byte, offset int) bool {
	if len(data) < eventHeaderSize+16 || data[8] != indexFileEventTypeCode {
		return false
	}
	eventLength := int(binary.LittleEndian.Uint32(data[9:13]))
	nextPosition := int(binary.LittleEndian.Uint32(data[13:17]))
	return eventLength > eventHeaderSize+16 && eventLength <= len(data) && nextPosition == offset+eventLength
}
