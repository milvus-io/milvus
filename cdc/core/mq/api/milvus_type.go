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

package api

import (
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/cdc/core/pb"
)

// Timestamp is an alias of uint64
type Timestamp = uint64

// IntPrimaryKey is an alias of int64
type IntPrimaryKey = int64

// UniqueID is an alias of int64
type UniqueID = int64

// MsgType is an alias of commonpb.MsgType
type MsgType = commonpb.MsgType

// MarshalType is an empty interface
type MarshalType = interface{}

type MsgPosition = pb.MsgPosition

type SubscriptionInitialPosition int

const (
	// SubscriptionPositionLatest is latest position which means the start consuming position will be the last message
	SubscriptionPositionLatest SubscriptionInitialPosition = iota

	// SubscriptionPositionEarliest is earliest position which means the start consuming position will be the first message
	SubscriptionPositionEarliest

	// SubscriptionPositionUnkown indicates we don't care about the consumer location, since we are doing another seek or only some meta api over that
	SubscriptionPositionUnknown
)

const DefaultPartitionIdx = 0
