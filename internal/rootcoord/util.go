// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package rootcoord

import (
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// EqualKeyPairArray check whether 2 KeyValuePairs are equal
func EqualKeyPairArray(p1 []*commonpb.KeyValuePair, p2 []*commonpb.KeyValuePair) bool {
	if len(p1) != len(p2) {
		return false
	}
	m1 := make(map[string]string)
	for _, p := range p1 {
		m1[p.Key] = p.Value
	}
	for _, p := range p2 {
		val, ok := m1[p.Key]
		if !ok {
			return false
		}
		if val != p.Value {
			return false
		}
	}
	return true
}

// GetFieldSchemaByID return field schema by id
func GetFieldSchemaByID(coll *etcdpb.CollectionInfo, fieldID typeutil.UniqueID) (*schemapb.FieldSchema, error) {
	for _, f := range coll.Schema.Fields {
		if f.FieldID == fieldID {
			return f, nil
		}
	}
	return nil, fmt.Errorf("field id = %d not found", fieldID)
}

// GetFieldSchemaByIndexID return field schema by it's index id
func GetFieldSchemaByIndexID(coll *etcdpb.CollectionInfo, idxID typeutil.UniqueID) (*schemapb.FieldSchema, error) {
	var fieldID typeutil.UniqueID
	exist := false
	for _, f := range coll.FieldIndexes {
		if f.IndexID == idxID {
			fieldID = f.FiledID
			exist = true
			break
		}
	}
	if !exist {
		return nil, fmt.Errorf("index id = %d is not attach to any field", idxID)
	}
	return GetFieldSchemaByID(coll, fieldID)
}

// EncodeDdOperation serialize DdOperation into string
func EncodeDdOperation(m proto.Message, ddType string) (string, error) {
	mByte, err := proto.Marshal(m)
	if err != nil {
		return "", err
	}
	ddOp := DdOperation{
		Body: mByte,
		Type: ddType,
	}
	ddOpByte, err := json.Marshal(ddOp)
	if err != nil {
		return "", err
	}
	return string(ddOpByte), nil
}

// DecodeDdOperation deserialize string to DdOperation
func DecodeDdOperation(str string, ddOp *DdOperation) error {
	return json.Unmarshal([]byte(str), ddOp)
}

// SegmentIndexInfoEqual return true if SegmentIndexInfos are identical
func SegmentIndexInfoEqual(info1 *etcdpb.SegmentIndexInfo, info2 *etcdpb.SegmentIndexInfo) bool {
	return info1.CollectionID == info2.CollectionID &&
		info1.PartitionID == info2.PartitionID &&
		info1.SegmentID == info2.SegmentID &&
		info1.FieldID == info2.FieldID &&
		info1.IndexID == info2.IndexID &&
		info1.EnableIndex == info2.EnableIndex
}

// EncodeMsgPositions serialize []*MsgPosition into string
func EncodeMsgPositions(msgPositions []*msgstream.MsgPosition) (string, error) {
	if len(msgPositions) == 0 {
		return "", nil
	}
	resByte, err := json.Marshal(msgPositions)
	if err != nil {
		return "", err
	}
	return string(resByte), nil
}

// DecodeMsgPositions deserialize string to []*MsgPosition
func DecodeMsgPositions(str string, msgPositions *[]*msgstream.MsgPosition) error {
	if str == "" || str == "null" {
		return nil
	}
	return json.Unmarshal([]byte(str), msgPositions)
}

// ToPhysicalChannel get physical channel name from virtual channel name
func ToPhysicalChannel(vchannel string) string {
	var idx int
	for idx = len(vchannel) - 1; idx >= 0; idx-- {
		if vchannel[idx] == '_' {
			break
		}
	}
	if idx < 0 {
		return vchannel
	}
	return vchannel[:idx]
}
