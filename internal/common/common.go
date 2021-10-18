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

package common

// system filed id:
// 0: unique row id
// 1: timestamp
// 100: first user field id
// 101: second user field id
// 102: ...

const (
	// StartOfUserFieldID represents the starting ID of the user-defined field
	StartOfUserFieldID = 100

	// RowIDField is the ID of the RowID field reserved by the system
	RowIDField = 0

	// TimeStampField is the ID of the Timestamp field reserved by the system
	TimeStampField = 1

	// RowIDFieldName defines the name of the RowID field
	RowIDFieldName = "RowID"

	// TimeStampFieldName defines the name of the Timestamp field
	TimeStampFieldName = "Timestamp"

	// DefaultShardsNum defines the default number of shards when creating a collection
	DefaultShardsNum = int32(2)
)
