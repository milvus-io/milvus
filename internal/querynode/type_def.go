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

package querynode

import (
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	rowIDFieldID     FieldID = 0
	timestampFieldID FieldID = 1
)

const invalidTimestamp = Timestamp(0)

type (
	UniqueID = typeutil.UniqueID
	// Timestamp is timestamp
	Timestamp = typeutil.Timestamp
	FieldID   = int64
	// IntPrimaryKey is the primary key of int type
	IntPrimaryKey = typeutil.IntPrimaryKey
	// DSL is the Domain Specific Language
	DSL = string
	// Channel is the virtual channel
	Channel = string
	// ConsumeSubName is consumer's subscription name of the message stream
	ConsumeSubName = string
)

type TimeRange struct {
	timestampMin Timestamp
	timestampMax Timestamp
}
