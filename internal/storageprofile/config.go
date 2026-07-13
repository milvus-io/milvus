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

package storageprofile

import (
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func PolicyConfigFromParamTable(params *paramtable.ComponentParam) (PolicyConfig, error) {
	if params == nil {
		params = paramtable.Get()
	}
	taskTypes, ok := ValidateTaskTypes(params.StorageProfileCfg.TaskTypes.GetAsStrings())
	if !ok {
		return PolicyConfig{}, merr.WrapErrServiceInternalMsg(
			"storage.profile.task.types contains an unknown task type: %v",
			params.StorageProfileCfg.TaskTypes.GetAsStrings(),
		)
	}
	level := ParseProfileLevel(params.StorageProfileCfg.Level.GetValue())
	if level == StorageProfileDisabled && params.StorageProfileCfg.Level.GetValue() != "disabled" {
		return PolicyConfig{}, merr.WrapErrServiceInternalMsg(
			"storage.profile.level must be one of disabled, summary, detailed: %s",
			params.StorageProfileCfg.Level.GetValue(),
		)
	}
	return PolicyConfig{
		Enabled:                      params.StorageProfileCfg.Enabled.GetAsBool(),
		Level:                        level,
		RequestAllowExplicit:         params.StorageProfileCfg.RequestAllowExplicit.GetAsBool(),
		TaskEnabled:                  params.StorageProfileCfg.TaskEnabled.GetAsBool(),
		TaskTypes:                    taskTypes,
		CacheEnabled:                 params.StorageProfileCfg.CacheEnabled.GetAsBool(),
		MaxActiveScopes:              int64(params.StorageProfileCfg.MaxActiveScopes.GetAsInt()),
		MaxProfiledRequestsPerSecond: int64(params.StorageProfileCfg.MaxProfiledRequestsPerSecond.GetAsInt()),
		MaxProfiledTasks:             int64(params.StorageProfileCfg.MaxProfiledTasks.GetAsInt()),
	}, nil
}
