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

package typeutil

type Timestamp = uint64
type IntPrimaryKey = int64
type UniqueID = int64

const (
	MasterServiceRole = "MasterService"
	ProxyNodeRole     = "ProxyNode"
	QueryServiceRole  = "QueryService"
	QueryNodeRole     = "QueryNode"
	IndexServiceRole  = "IndexService"
	IndexNodeRole     = "IndexNode"
	DataServiceRole   = "DataService"
	DataNodeRole      = "DataNode"
)
