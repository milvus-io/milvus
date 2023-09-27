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

package paramtable

import (
	"strconv"
	"sync"
	"time"
)

const (
	runtimeNodeIDKey     = "runtime.nodeID"
	runtimeRoleKey       = "runtime.role"
	runtimeCreateTimeKey = "runtime.createTime"
	runtimeUpdateTimeKey = "runtime.updateTime"
)

var (
	once       sync.Once
	params     ComponentParam
	hookParams hookConfig
)

func Init() {
	once.Do(func() {
		baseTable := NewBaseTable()
		params.Init(baseTable)
		hookBaseTable := NewBaseTableFromYamlOnly(hookYamlFile)
		hookParams.init(hookBaseTable)
	})
}

func InitWithBaseTable(baseTable *BaseTable) {
	once.Do(func() {
		params.Init(baseTable)
		hookBaseTable := NewBaseTableFromYamlOnly(hookYamlFile)
		hookParams.init(hookBaseTable)
	})
}

func Get() *ComponentParam {
	return &params
}

func GetBaseTable() *BaseTable {
	return params.baseTable
}

func GetHookParams() *hookConfig {
	return &hookParams
}

func SetNodeID(newID UniqueID) {
	params.baseTable.Save(runtimeNodeIDKey, strconv.FormatInt(newID, 10))
}

func GetNodeID() UniqueID {
	nodeID, err := strconv.ParseInt(params.baseTable.Get(runtimeNodeIDKey), 10, 64)
	if err != nil {
		return 0
	}
	return nodeID
}

func SetRole(role string) {
	params.baseTable.Save(runtimeRoleKey, role)
}

func GetRole() string {
	if params.baseTable == nil {
		return ""
	}
	return params.baseTable.Get(runtimeRoleKey)
}

func SetCreateTime(d time.Time) {
	params.baseTable.Save(runtimeCreateTimeKey, strconv.FormatInt(d.UnixNano(), 10))
}

func GetCreateTime() time.Time {
	v, _ := strconv.ParseInt(params.baseTable.Get(runtimeCreateTimeKey), 10, 64)
	return time.Unix(v/1e9, v%1e9)
}

func SetUpdateTime(d time.Time) {
	params.baseTable.Save(runtimeUpdateTimeKey, strconv.FormatInt(d.UnixNano(), 10))
}

func GetUpdateTime() time.Time {
	v, _ := strconv.ParseInt(params.baseTable.Get(runtimeUpdateTimeKey), 10, 64)
	return time.Unix(v/1e9, v%1e9)
}
