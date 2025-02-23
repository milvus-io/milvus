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

package indexparamcheck

import (
	"strconv"

	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

// TODO: add more test cases which `IndexChecker.CheckTrain` return false,
//       for example, maybe we can copy test cases from regression test later.

func invalidIVFParamsMin() map[string]string {
	invalidIVFParams := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(MinNList - 1),
		Metric: metric.L2,
	}
	return invalidIVFParams
}

func invalidIVFParamsMax() map[string]string {
	invalidIVFParams := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(MaxNList + 1),
		Metric: metric.L2,
	}
	return invalidIVFParams
}

func copyParams(original map[string]string) map[string]string {
	result := make(map[string]string)
	for key, value := range original {
		result[key] = value
	}
	return result
}
