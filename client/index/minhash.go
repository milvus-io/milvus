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

import (
	"strconv"

	"github.com/milvus-io/milvus/client/v2/entity"
)

var (
	minhashElementBitWidthKey = `mh_element_bit_width`
	minhashLSHBandKey         = `mh_lsh_band`
	minhashLSHCodeInMemKey    = `mh_lsh_code_in_mem`
	minhashWithRawdataKey     = `with_raw_data`
	minhashLSHBloomFPProbKey  = `mh_lsh_bloom_false_positive_prob`

	minhashSearchWithJaccardKey = `mh_search_with_jaccard`
	minhashRefineK              = `refine_k`
	minhashLSHBatchSearchKey    = `mh_lsh_batch_search`
)

type minhashLSHIndex struct {
	baseIndex

	lshBand int
}

func (idx *minhashLSHIndex) WithElementBitWidth(elementBitWidth int) *minhashLSHIndex {
	idx.params[minhashElementBitWidthKey] = strconv.Itoa(elementBitWidth)
	return idx
}

func (idx *minhashLSHIndex) WithLSHCodeInMem(lshCodeInMem int) *minhashLSHIndex {
	idx.params[minhashLSHCodeInMemKey] = strconv.Itoa(lshCodeInMem)
	return idx
}

func (idx *minhashLSHIndex) WithRawData(withRawData bool) *minhashLSHIndex {
	idx.params[minhashWithRawdataKey] = strconv.FormatBool(withRawData)
	return idx
}

func (idx *minhashLSHIndex) WithBloomFilterFalsePositiveProb(fpProb float64) *minhashLSHIndex {
	idx.params[minhashLSHBloomFPProbKey] = strconv.FormatFloat(fpProb, 'f', -1, 64)
	return idx
}

func (idx *minhashLSHIndex) Params() map[string]string {
	result := map[string]string{
		MetricTypeKey:     string(idx.metricType),
		IndexTypeKey:      string(MinHashLSH),
		minhashLSHBandKey: strconv.Itoa(idx.lshBand),
	}

	for key, value := range idx.params {
		result[key] = value
	}

	return result
}

func NewMinHashLSHIndex(metricType entity.MetricType, lshBand int) *minhashLSHIndex {
	idx := &minhashLSHIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  MinHashLSH,
			params:     make(map[string]string),
		},
		lshBand: lshBand,
	}

	return idx
}

type minHashLSHAnnParam struct {
	baseAnnParam
}

func (ap *minHashLSHAnnParam) WithSearchWithJACCARD(searchWithJACCARD bool) *minHashLSHAnnParam {
	ap.params[minhashSearchWithJaccardKey] = strconv.FormatBool(searchWithJACCARD)
	return ap
}

func (ap *minHashLSHAnnParam) WithRefineK(refineK int) *minHashLSHAnnParam {
	ap.params[minhashRefineK] = strconv.Itoa(refineK)
	return ap
}

func (ap *minHashLSHAnnParam) WithBatchSearch(batchSearch bool) *minHashLSHAnnParam {
	ap.params[minhashLSHBatchSearchKey] = strconv.FormatBool(batchSearch)
	return ap
}

func (ap *minHashLSHAnnParam) Params() map[string]any {
	result := ap.baseAnnParam.Params()
	return result
}

func NewMinHashLSHAnnParam() *minHashLSHAnnParam {
	return &minHashLSHAnnParam{
		baseAnnParam: baseAnnParam{
			params: make(map[string]any),
		},
	}
}
