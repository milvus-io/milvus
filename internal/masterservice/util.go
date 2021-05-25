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

package masterservice

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

//return
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

func GetFieldSchemaByID(coll *etcdpb.CollectionInfo, fieldID typeutil.UniqueID) (*schemapb.FieldSchema, error) {
	for _, f := range coll.Schema.Fields {
		if f.FieldID == fieldID {
			return f, nil
		}
	}
	return nil, fmt.Errorf("field id = %d not found", fieldID)
}

//GetFieldSchemaByIndexID return the field schema by it's index id
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
func EncodeDdOperation(m proto.Message, m1 proto.Message, ddType string) (string, error) {
	mStr := proto.MarshalTextString(m)
	m1Str := proto.MarshalTextString(m1)
	ddOp := DdOperation{
		Body:  mStr,
		Body1: m1Str, // used for DdCreateCollection only
		Type:  ddType,
	}
	ddOpByte, err := json.Marshal(ddOp)
	if err != nil {
		return "", err
	}
	return string(ddOpByte), nil
}

// SegmentIndexInfoEqual return true if 2 SegmentIndexInfo are identical
func SegmentIndexInfoEqual(info1 *etcdpb.SegmentIndexInfo, info2 *etcdpb.SegmentIndexInfo) bool {
	return info1.SegmentID == info2.SegmentID &&
		info1.FieldID == info2.FieldID &&
		info1.IndexID == info2.IndexID &&
		info1.BuildID == info2.BuildID &&
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
