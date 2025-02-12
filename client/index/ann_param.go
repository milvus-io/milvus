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

type AnnParam interface {
	Params() map[string]any
}

type baseAnnParam struct {
	params map[string]any
}

func (b baseAnnParam) WithExtraParam(key string, value any) {
	b.params[key] = value
}

func (b baseAnnParam) Params() map[string]any {
	return b.params
}

func (b baseAnnParam) WithRadius(radius float64) {
	b.WithExtraParam("radius", radius)
}

func (b baseAnnParam) WithRangeFilter(rangeFilter float64) {
	b.WithExtraParam("range_filter", rangeFilter)
}

type CustomAnnParam struct {
	baseAnnParam
}

func NewCustomAnnParam() CustomAnnParam {
	return CustomAnnParam{
		baseAnnParam: baseAnnParam{
			params: make(map[string]any),
		},
	}
}
