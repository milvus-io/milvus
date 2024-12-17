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

import "strconv"

const (
	scannNlistKey       = `nlist`
	scannWithRawDataKey = `with_raw_data`
	scannNProbeKey      = `nprobe`
	scannReorderKKey    = `reorder_k`
)

type scannIndex struct {
	baseIndex

	nlist       int
	withRawData bool
}

func (idx scannIndex) Params() map[string]string {
	return map[string]string{
		MetricTypeKey:       string(idx.metricType),
		IndexTypeKey:        string(SCANN),
		scannNlistKey:       strconv.Itoa(idx.nlist),
		scannWithRawDataKey: strconv.FormatBool(idx.withRawData),
	}
}

func NewSCANNIndex(metricType MetricType, nlist int, withRawData bool) Index {
	return scannIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  SCANN,
		},
		nlist:       nlist,
		withRawData: withRawData,
	}
}

type scannAnnParam struct {
	baseAnnParam
	nprobe   int
	reorderK int
}

func NewSCANNAnnParam(nprobe int, reorderK int) scannAnnParam {
	return scannAnnParam{
		baseAnnParam: baseAnnParam{
			params: make(map[string]any),
		},
		nprobe:   nprobe,
		reorderK: reorderK,
	}
}

func (ap scannAnnParam) Params() map[string]any {
	result := ap.baseAnnParam.params
	result[scannNProbeKey] = ap.nprobe
	result[scannReorderKKey] = ap.reorderK
	return result
}
