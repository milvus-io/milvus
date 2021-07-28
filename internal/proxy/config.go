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

package proxy

import (
	"strings"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typedef"
)

// const
var scorePositivelyRelatedToDistanceMetrics = []typedef.MetricType{
	typedef.MetricIP,
	typedef.MetricJaccard,
	typedef.MetricTanimoto,
}
var scoreNegativelyRelatedToDistanceMetrics = []typedef.MetricType{
	typedef.MetricL2,
	typedef.MetricHamming,
	typedef.MetricSuperStructure,
	typedef.MetricSubStructure,
}

func inScorePositivelyRelatedToDistanceMetrics(metric typedef.MetricType) bool {
	return funcutil.SliceContain(scorePositivelyRelatedToDistanceMetrics, strings.ToUpper(metric))
}

func inScoreNegativelyRelatedToDistanceMetrics(metric typedef.MetricType) bool {
	return funcutil.SliceContain(scoreNegativelyRelatedToDistanceMetrics, strings.ToUpper(metric))
}
