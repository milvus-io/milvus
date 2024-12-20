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
	diskANNSearchListKey = `search_list`
)

var _ Index = diskANNIndex{}

type diskANNIndex struct {
	baseIndex
}

func (idx diskANNIndex) Params() map[string]string {
	return map[string]string{
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(DISKANN),
	}
}

func NewDiskANNIndex(metricType MetricType) Index {
	return &diskANNIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  DISKANN,
		},
	}
}

type diskANNParam struct {
	baseAnnParam
	searchList int
}

func NewDiskAnnParam(searchList int) diskANNParam {
	return diskANNParam{
		baseAnnParam: baseAnnParam{
			params: make(map[string]any),
		},
		searchList: searchList,
	}
}

func (ap diskANNParam) Params() map[string]any {
	result := ap.baseAnnParam.params
	result[diskANNSearchListKey] = ap.searchList
	return result
}
