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

package analyzer

import (
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/analyzer/canalyzer"
	"github.com/milvus-io/milvus/internal/util/analyzer/interfaces"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type (
	Analyzer    interfaces.Analyzer
	TokenStream interfaces.TokenStream
)

func NewAnalyzer(param string, extraInfo string) (Analyzer, error) {
	return canalyzer.NewAnalyzer(param, extraInfo)
}

func ValidateAnalyzer(param string, extraInfo string) ([]int64, error) {
	return canalyzer.ValidateAnalyzer(param, extraInfo)
}

func UpdateGlobalResourceInfo(resourceMap map[string]int64) error {
	return canalyzer.UpdateGlobalResourceInfo(resourceMap)
}

func BuildExtraResourceInfo(storage string, resources []*internalpb.FileResourceInfo) (string, error) {
	return canalyzer.BuildExtraResourceInfo(storage, resources)
}

func InitOptions() error {
	return canalyzer.InitOptions()
}

// WithFileAccessCheck adds admission checking without changing resource bindings.
func WithFileAccessCheck(extraInfo string) (string, error) {
	extra := make(map[string]json.RawMessage)
	if extraInfo != "" {
		if err := json.Unmarshal([]byte(extraInfo), &extra); err != nil || extra == nil {
			return "", merr.WrapErrServiceInternalMsg("invalid analyzer runtime options")
		}
	}
	extra["check_file_access"] = json.RawMessage("true")
	data, err := json.Marshal(extra)
	return string(data), err
}
