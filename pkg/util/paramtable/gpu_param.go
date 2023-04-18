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

package paramtable

// /////////////////////////////////////////////////////////////////////////////
type gpuConfig struct {
	InitMemPoolSize ParamItem `refreshable:"false"`
	MaxMemPoolSize  ParamItem `refreshable:"false"`
}

func (p *gpuConfig) init(base *BaseTable) {
	p.InitMemPoolSize = ParamItem{
		Key:          "gpu.initMemPoolSize",
		Version:      "2.3.0",
		DefaultValue: "2048",
		Doc:          "initial gpu memory pool size (MB)",
		Export:       true,
	}
	p.InitMemPoolSize.Init(base.mgr)

	p.MaxMemPoolSize = ParamItem{
		Key:          "gpu.maxMemPoolSize",
		Version:      "2.3.0",
		DefaultValue: "4096",
		Doc:          "maximum gpu memory pool size (MB)",
		Export:       true,
	}
	p.MaxMemPoolSize.Init(base.mgr)
}
