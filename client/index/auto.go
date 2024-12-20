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

package index

const (
	autoLevelKey = `level`
)

var _ Index = autoIndex{}

type autoIndex struct {
	baseIndex
}

func (idx autoIndex) Params() map[string]string {
	return map[string]string{
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(AUTOINDEX),
	}
}

func NewAutoIndex(metricType MetricType) Index {
	return autoIndex{
		baseIndex: baseIndex{
			indexType:  AUTOINDEX,
			metricType: metricType,
		},
	}
}

type autoAnnParam struct {
	baseAnnParam
	level int
}

func NewAutoAnnParam(level int) autoAnnParam {
	return autoAnnParam{
		baseAnnParam: baseAnnParam{
			params: make(map[string]any),
		},
		level: level,
	}
}

func (ap autoAnnParam) Params() map[string]any {
	result := ap.baseAnnParam.params
	result[autoLevelKey] = ap.level
	return result
}
