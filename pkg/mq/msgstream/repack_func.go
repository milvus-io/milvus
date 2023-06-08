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

package msgstream

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

// InsertRepackFunc is used to repack messages after hash by primary key
func InsertRepackFunc(tsMsgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error) {
	result := make(map[int32]*MsgPack)
	for i, request := range tsMsgs {
		if request.Type() != commonpb.MsgType_Insert {
			return nil, errors.New("msg's must be Insert")
		}
		insertRequest := request.(*InsertMsg)
		keys := hashKeys[i]

		keysLen := len(keys)

		if err := insertRequest.CheckAligned(); err != nil {
			return nil, err
		}
		if insertRequest.NRows() != uint64(keysLen) {
			return nil, errors.New("the length of hashValue, timestamps, rowIDs, RowData are not equal")
		}
		for index, key := range keys {
			_, ok := result[key]
			if !ok {
				msgPack := MsgPack{}
				result[key] = &msgPack
			}

			insertMsg := insertRequest.IndexMsg(index)
			result[key].Msgs = append(result[key].Msgs, insertMsg)
		}
	}
	return result, nil
}

// DeleteRepackFunc is used to repack messages after hash by primary key
func DeleteRepackFunc(tsMsgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error) {
	result := make(map[int32]*MsgPack)
	for i, request := range tsMsgs {
		if request.Type() != commonpb.MsgType_Delete {
			return nil, errors.New("msg's must be Delete")
		}
		deleteRequest := request.(*DeleteMsg)
		keys := hashKeys[i]

		timestampLen := len(deleteRequest.Timestamps)
		keysLen := len(keys)

		if keysLen != timestampLen || int64(keysLen) != deleteRequest.NumRows {
			return nil, errors.New("the length of hashValue, timestamps, primaryKeys are not equal")
		}

		key := keys[0]
		_, ok := result[key]
		if !ok {
			result[key] = &MsgPack{}
		}
		result[key].Msgs = append(result[key].Msgs, request)
	}
	return result, nil
}

// DefaultRepackFunc is used to repack messages after hash by primary key
func DefaultRepackFunc(tsMsgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error) {
	if len(hashKeys) < len(tsMsgs) {
		return nil, fmt.Errorf(
			"the length of hash keys (%d) is less than the length of messages (%d)",
			len(hashKeys),
			len(tsMsgs),
		)
	}

	// after assigning segment id to msg, tsMsgs was already re-bucketed
	pack := make(map[int32]*MsgPack)
	for idx, msg := range tsMsgs {
		if len(hashKeys[idx]) <= 0 {
			return nil, fmt.Errorf("no hash key for %dth message", idx)
		}
		key := hashKeys[idx][0]
		_, ok := pack[key]
		if !ok {
			pack[key] = &MsgPack{}
		}
		pack[key].Msgs = append(pack[key].Msgs, msg)
	}
	return pack, nil
}
