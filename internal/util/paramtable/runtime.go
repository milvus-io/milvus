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
)

const (
	runtimeNodeIDKey = "runtime.nodeID"
	runtimeRoleKey   = "runtime.role"
)

var params ComponentParam

func Init() {
	params.InitOnce()
}

func Get() *ComponentParam {
	return &params
}

func SetNodeID(newID UniqueID) {
	params.Save(runtimeNodeIDKey, strconv.FormatInt(newID, 10))
}

func GetNodeID() UniqueID {
	nodeID, err := strconv.ParseInt(params.Get(runtimeNodeIDKey), 10, 64)
	if err != nil {
		return 0
	}
	return nodeID
}

func SetRole(role string) {
	params.Save(runtimeRoleKey, role)
}

func GetRole() string {
	return params.Get(runtimeRoleKey)
}
