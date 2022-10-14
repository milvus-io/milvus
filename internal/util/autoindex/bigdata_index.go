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

package autoindex

import (
	"encoding/json"
	"strconv"
)

type BigDataIndexExtraParams struct {
	PGCodeBudgetGBRatio      float64
	BuildNumThreadsRatio     float64
	SearchCacheBudgetGBRatio float64
	LoadNumThreadRatio       float64
	BeamWidthRatio           float64
}

const (
	BuildRatioKey                   = "build_ratio"
	PrepareRatioKey                 = "prepare_ratio"
	BeamWidthRatioKey               = "beamwidth_ratio"
	DefaultPGCodeBudgetGBRatio      = 0.125
	DefaultBuildNumThreadsRatio     = 1.0
	DefaultSearchCacheBudgetGBRatio = 0.125
	DefaultLoadNumThreadRatio       = 8.0
	DefaultBeamWidthRatio           = 4.0
)

func NewBigDataIndexExtraParams() *BigDataIndexExtraParams {
	ret := &BigDataIndexExtraParams{
		PGCodeBudgetGBRatio:      DefaultPGCodeBudgetGBRatio,
		BuildNumThreadsRatio:     DefaultBuildNumThreadsRatio,
		SearchCacheBudgetGBRatio: DefaultSearchCacheBudgetGBRatio,
		LoadNumThreadRatio:       DefaultLoadNumThreadRatio,
		BeamWidthRatio:           DefaultBeamWidthRatio,
	}
	return ret
}

func NewBigDataExtraParamsFromJSON(jsonStr string) (*BigDataIndexExtraParams, error) {
	buffer := make(map[string]string)
	err := json.Unmarshal([]byte(jsonStr), &buffer)
	if err != nil {
		return nil, err
	}
	return NewBigDataExtraParamsFromMap(buffer)
}

func NewBigDataExtraParamsFromMap(value map[string]string) (*BigDataIndexExtraParams, error) {
	ret := &BigDataIndexExtraParams{}
	var err error
	buildRatio, ok := value[BuildRatioKey]
	if !ok {
		ret.PGCodeBudgetGBRatio = DefaultPGCodeBudgetGBRatio
		ret.BuildNumThreadsRatio = DefaultBuildNumThreadsRatio
	} else {
		valueMap1 := make(map[string]float64)
		err = json.Unmarshal([]byte(buildRatio), &valueMap1)
		if err != nil {
			return ret, err
		}

		PGCodeBudgetGBRatio, ok := valueMap1["pg_code_budget_gb"]
		if !ok {
			ret.PGCodeBudgetGBRatio = DefaultPGCodeBudgetGBRatio
		} else {
			ret.PGCodeBudgetGBRatio = PGCodeBudgetGBRatio
		}
		BuildNumThreadsRatio, ok := valueMap1["num_threads"]
		if !ok {
			ret.BuildNumThreadsRatio = DefaultBuildNumThreadsRatio
		} else {
			ret.BuildNumThreadsRatio = BuildNumThreadsRatio
		}
	}

	prepareRatio, ok := value[PrepareRatioKey]
	if !ok {
		ret.SearchCacheBudgetGBRatio = DefaultSearchCacheBudgetGBRatio
		ret.LoadNumThreadRatio = DefaultLoadNumThreadRatio
	} else {
		valueMap2 := make(map[string]float64)
		err = json.Unmarshal([]byte(prepareRatio), &valueMap2)
		if err != nil {
			return ret, err
		}
		SearchCacheBudgetGBRatio, ok := valueMap2["search_cache_budget_gb"]
		if !ok {
			ret.SearchCacheBudgetGBRatio = DefaultSearchCacheBudgetGBRatio
		} else {
			ret.SearchCacheBudgetGBRatio = SearchCacheBudgetGBRatio
		}
		LoadNumThreadRatio, ok := valueMap2["num_threads"]
		if !ok {
			ret.LoadNumThreadRatio = DefaultLoadNumThreadRatio
		} else {
			ret.LoadNumThreadRatio = LoadNumThreadRatio
		}
	}
	beamWidthRatioStr, ok := value[BeamWidthRatioKey]
	if !ok {
		ret.BeamWidthRatio = DefaultBeamWidthRatio
	} else {
		beamWidthRatio, err := strconv.ParseFloat(beamWidthRatioStr, 64)
		if err != nil {
			ret.BeamWidthRatio = DefaultBeamWidthRatio
		} else {
			ret.BeamWidthRatio = beamWidthRatio
		}
	}
	return ret, nil
}
