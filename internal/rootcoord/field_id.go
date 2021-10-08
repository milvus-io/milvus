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

import "github.com/milvus-io/milvus/internal/common"

// system filed id:
// 0: unique row id
// 1: timestamp
// 100: first user field id
// 101: second user field id
// 102: ...

const (
	// StartOfUserFieldID id of user defined filed begin from here
	StartOfUserFieldID = common.StartOfUserFieldID

	// RowIDField id of row ID field
	RowIDField = common.RowIDField

	// TimeStampField id of timestamp field
	TimeStampField = common.TimeStampField

	// RowIDFieldName name of row ID field
	RowIDFieldName = common.RowIDFieldName

	// TimeStampFieldName name of timestamp field
	TimeStampFieldName = common.TimeStampFieldName
)
