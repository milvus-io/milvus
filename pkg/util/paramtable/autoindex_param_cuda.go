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

//go:build cuda
// +build cuda

package paramtable

func GetIndexParam() ParamItem {
	return ParamItem{
		Key:          "autoIndex.params.build",
		Version:      "2.2.0",
		DefaultValue: `{"intermediate_graph_degree":64, "graph_degree": 32, "index_type": "GPU_CAGRA", "metric_type": "COSINE"}`,
		Formatter:    GetBuildParamFormatter(FloatVectorDefaultMetricType, "autoIndex.params.build"),
		Export:       true,
	}
}
