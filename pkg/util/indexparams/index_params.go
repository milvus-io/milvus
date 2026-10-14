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

package indexparams

import (
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var configableIndexParams = typeutil.NewSet[string]()

func init() {
	configableIndexParams.Insert(common.MmapEnabledKey)
	configableIndexParams.Insert(common.IndexOffsetCacheEnabledKey)
	configableIndexParams.Insert(common.WarmupKey)
}

func IsConfigableIndexParam(key string) bool {
	return configableIndexParams.Contain(key)
}

func SetBitmapIndexLoadParams(params *paramtable.ComponentParam, indexParams map[string]string) {
	_, exist := indexParams[common.IndexOffsetCacheEnabledKey]
	if exist {
		return
	}
	indexParams[common.IndexOffsetCacheEnabledKey] = params.QueryNodeCfg.IndexOffsetCacheEnabled.GetValue()
}

// AppendPrepareLoadParams applies deployment load defaults without replacing
// explicit index values. Load and Reopen consume fresh metadata each time.
func AppendPrepareLoadParams(params *paramtable.ComponentParam, indexParams map[string]string) error {
	defaults := make(map[string]string)
	if params.AutoIndexConfig.Enable.GetAsBool() {
		for k, v := range params.AutoIndexConfig.PrepareParams.GetAsJSONMap() {
			defaults[k] = v
		}
		for k, v := range params.AutoIndexConfig.LoadAdaptParams.GetAsJSONMap() {
			defaults[k] = v
		}
	}
	if params.KnowhereConfig.Enable.GetAsBool() {
		if _, err := params.KnowhereConfig.MergeIndexParams(indexParams[common.IndexTypeKey], paramtable.LoadStage, defaults); err != nil {
			return err
		}
	}
	for key, value := range defaults {
		if _, present := indexParams[key]; !present {
			indexParams[key] = value
		}
	}
	return nil
}

// GetRuntimeLoadParams returns only additions for this load operation. Persisted
// index choices remain separate so static artifact capabilities cannot observe
// deployment load defaults. Presence, including empty/zero/false, wins.
func GetRuntimeLoadParams(params *paramtable.ComponentParam, indexParams map[string]string) (map[string]string, error) {
	effective := make(map[string]string, len(indexParams))
	for key, value := range indexParams {
		effective[key] = value
	}
	if indexParams[common.IndexTypeKey] == "BITMAP" {
		SetBitmapIndexLoadParams(params, effective)
	}
	if err := AppendPrepareLoadParams(params, effective); err != nil {
		return nil, err
	}
	additions := make(map[string]string)
	for key, value := range effective {
		if _, explicit := indexParams[key]; !explicit {
			additions[key] = value
		}
	}
	return additions, nil
}
