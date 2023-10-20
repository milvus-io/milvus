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

package proxy

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

// insertRepackFunc deprecated, use defaultInsertRepackFunc instead.
func insertRepackFunc(
	tsMsgs []msgstream.TsMsg,
	hashKeys [][]int32,
) (map[int32]*msgstream.MsgPack, error) {
	if len(hashKeys) < len(tsMsgs) {
		return nil, fmt.Errorf(
			"the length of hash keys (%d) is less than the length of messages (%d)",
			len(hashKeys),
			len(tsMsgs),
		)
	}

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
		} else {
			return nil, fmt.Errorf("no hash key for %dth message", i)
		}
	}

	return result, nil
}

// defaultInsertRepackFunc repacks the dml messages.
func defaultInsertRepackFunc(
	tsMsgs []msgstream.TsMsg,
	hashKeys [][]int32,
) (map[int32]*msgstream.MsgPack, error) {
	if len(hashKeys) < len(tsMsgs) {
		return nil, fmt.Errorf(
			"the length of hash keys (%d) is less than the length of messages (%d)",
			len(hashKeys),
			len(tsMsgs),
		)
	}

	// after assigning segment id to msg, tsMsgs was already re-bucketed
	pack := make(map[int32]*msgstream.MsgPack)
	for idx, msg := range tsMsgs {
		if len(hashKeys[idx]) <= 0 {
			return nil, fmt.Errorf("no hash key for %dth message", idx)
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

func replicatePackFunc(
	tsMsgs []msgstream.TsMsg,
	hashKeys [][]int32,
) (map[int32]*msgstream.MsgPack, error) {
	return map[int32]*msgstream.MsgPack{
		0: {
			Msgs: tsMsgs,
		},
	}, nil
}
