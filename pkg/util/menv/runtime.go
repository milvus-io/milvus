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

package menv

import (
	"strconv"
	"time"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var runtimeParam *runtimeConfig

func init() {
	runtimeParam = newRuntimeConfig()
}

func newRuntimeConfig() *runtimeConfig {
	return &runtimeConfig{
		components: typeutil.NewConcurrentSet[string](),
	}
}

// runtimeConfig is just a private environment value table.
type runtimeConfig struct {
	createTime atomic.Time
	updateTime atomic.Time
	role       atomic.String
	nodeID     atomic.Int64
	components *typeutil.ConcurrentSet[string]
}

func SetNodeID(newID typeutil.UniqueID) {
	runtimeParam.nodeID.Store(newID)
}

func GetNodeID() typeutil.UniqueID {
	return runtimeParam.nodeID.Load()
}

func GetStringNodeID() string {
	return strconv.FormatInt(GetNodeID(), 10)
}

func SetRole(role string) {
	runtimeParam.role.Store(role)
}

func GetRole() string {
	return runtimeParam.role.Load()
}

func SetCreateTime(d time.Time) {
	runtimeParam.createTime.Store(d)
}

func GetCreateTime() time.Time {
	return runtimeParam.createTime.Load()
}

func SetUpdateTime(d time.Time) {
	runtimeParam.updateTime.Store(d)
}

func GetUpdateTime() time.Time {
	return runtimeParam.updateTime.Load()
}

func SetLocalComponentEnabled(component string) {
	runtimeParam.components.Insert(component)
}

func IsLocalComponentEnabled(component string) bool {
	return runtimeParam.components.Contain(component)
}
