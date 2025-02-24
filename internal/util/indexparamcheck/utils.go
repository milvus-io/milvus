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

package indexparamcheck

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// CheckIntByRange check if the data corresponding to the key is in the range of [min, max].
// Return false if:
//  1. the key does not exist, or
//  2. the data cannot be converted to an integer, or
//  3. the number is not in the range [min, max]
//
// Return true otherwise
func CheckIntByRange(params map[string]string, key string, min, max int) bool {
	valueStr, ok := params[key]
	if !ok {
		return false
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return false
	}

	return value >= min && value <= max
}

// CheckStrByValues check whether the data corresponding to the key appears in the string slice of container.
// Return false if:
//  1. the key does not exist, or
//  2. the data does not appear in the container
//
// Return true otherwise
func CheckStrByValues(params map[string]string, key string, container []string) bool {
	value, ok := params[key]
	if !ok {
		return false
	}

	return funcutil.SliceContain(container, value)
}

func errOutOfRange(x interface{}, lb interface{}, ub interface{}) error {
	return fmt.Errorf("%v out of range: [%v, %v]", x, lb, ub)
}

func setDefaultIfNotExist(params map[string]string, key string, defaultValue string) {
	_, exist := params[key]
	if !exist {
		params[key] = defaultValue
	}
}

func CheckAutoIndexHelper(key string, m map[string]string, dtype schemapb.DataType) {
	indexType, ok := m[common.IndexTypeKey]
	if !ok {
		panic(fmt.Sprintf("%s invalid, index type not found", key))
	}

	checker, err := GetIndexCheckerMgrInstance().GetChecker(indexType)
	if err != nil {
		panic(fmt.Sprintf("%s invalid, unsupported index type: %s", key, indexType))
	}

	if err := checker.StaticCheck(dtype, m); err != nil {
		panic(fmt.Sprintf("%s invalid, parameters invalid, error: %s", key, err.Error()))
	}
}

func CheckAutoIndexConfig() {
	autoIndexCfg := &paramtable.Get().AutoIndexConfig
	CheckAutoIndexHelper(autoIndexCfg.IndexParams.Key, autoIndexCfg.IndexParams.GetAsJSONMap(), schemapb.DataType_FloatVector)
	CheckAutoIndexHelper(autoIndexCfg.BinaryIndexParams.Key, autoIndexCfg.BinaryIndexParams.GetAsJSONMap(), schemapb.DataType_BinaryVector)
	CheckAutoIndexHelper(autoIndexCfg.SparseIndexParams.Key, autoIndexCfg.SparseIndexParams.GetAsJSONMap(), schemapb.DataType_SparseFloatVector)
}

func ValidateParamTable() {
	CheckAutoIndexConfig()
}
