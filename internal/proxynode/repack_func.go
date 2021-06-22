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

package proxy

import (
	"github.com/milvus-io/milvus/internal/msgstream"
)

func insertRepackFunc(tsMsgs []msgstream.TsMsg,
	hashKeys [][]int32) (map[int32]*msgstream.MsgPack, error) {

	result := make(map[int32]*msgstream.MsgPack)
	for i, request := range tsMsgs {
		keys := hashKeys[i]
		if len(keys) > 0 {
			key := keys[0]
			_, ok := result[key]
			if !ok {
				result[key] = &msgstream.MsgPack{}
			}
			result[key].Msgs = append(result[key].Msgs, request)
		}
	}
	return result, nil
}

func defaultInsertRepackFunc(tsMsgs []msgstream.TsMsg, hashKeys [][]int32) (map[int32]*msgstream.MsgPack, error) {
	// after assigning segment id to msg, tsMsgs was already re-bucketed
	pack := make(map[int32]*msgstream.MsgPack)
	for idx, msg := range tsMsgs {
		if len(hashKeys[idx]) <= 0 {
			continue
		}
		key := hashKeys[idx][0]
		_, ok := pack[key]
		if !ok {
			pack[key] = &msgstream.MsgPack{}
		}
		pack[key].Msgs = append(pack[key].Msgs, msg)
	}
	return pack, nil
}
