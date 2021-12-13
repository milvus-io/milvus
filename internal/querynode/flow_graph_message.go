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

package querynode

import (
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

// Msg is an interface which has a function named TimeTick
type Msg = flowgraph.Msg

// MsgStreamMsg is an implementation of interface Msg
type MsgStreamMsg = flowgraph.MsgStreamMsg

// insertMsg is an implementation of interface Msg
type insertMsg struct {
	insertMessages []*msgstream.InsertMsg
	deleteMessages []*msgstream.DeleteMsg
	timeRange      TimeRange
}

// deleteMsg is an implementation of interface Msg
type deleteMsg struct {
	deleteMessages []*msgstream.DeleteMsg
	timeRange      TimeRange
}

// serviceTimeMsg is an implementation of interface Msg
type serviceTimeMsg struct {
	timeRange TimeRange
}

// TimeTick returns timestamp of insertMsg
func (iMsg *insertMsg) TimeTick() Timestamp {
	return iMsg.timeRange.timestampMax
}

// TimeTick returns timestamp of deleteMsg
func (dMsg *deleteMsg) TimeTick() Timestamp {
	return dMsg.timeRange.timestampMax
}

// TimeTick returns timestamp of serviceTimeMsg
func (stMsg *serviceTimeMsg) TimeTick() Timestamp {
	return stMsg.timeRange.timestampMax
}
