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

package datacoord

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
)

var _ RWChannel = &channelMeta{}

type RWChannel interface {
	String() string
	GetName() string
	GetCollectionID() UniqueID
	GetStartPosition() *msgpb.MsgPosition
}

// TODO fubang same as StateChannel
type channelMeta struct {
	Name          string
	CollectionID  UniqueID
	StartPosition *msgpb.MsgPosition
}

func (ch *channelMeta) GetName() string {
	return ch.Name
}

func (ch *channelMeta) GetCollectionID() UniqueID {
	return ch.CollectionID
}

func (ch *channelMeta) GetStartPosition() *msgpb.MsgPosition {
	return ch.StartPosition
}

// String implement Stringer.
func (ch *channelMeta) String() string {
	// schema maybe too large to print
	return fmt.Sprintf("Name: %s, CollectionID: %d, StartPositions: %v", ch.Name, ch.CollectionID, ch.StartPosition)
}
