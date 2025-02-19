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

package pipeline

import (
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

type (
	// Node is flowgraph.Node
	Node = flowgraph.Node

	// BaseNode is flowgraph.BaseNode
	BaseNode = flowgraph.BaseNode

	// InputNode is flowgraph.InputNode
	InputNode = flowgraph.InputNode
)

var flowGraphRetryOpt = retry.Attempts(20)

var fgRetryOptVal atomic.Value

func init() {
	setFlowGraphRetryOpt(retry.Attempts(20))
}

// setFlowGraphRetryOpt set retry option for flowgraph
// used for tests only
func setFlowGraphRetryOpt(opt retry.Option) {
	fgRetryOptVal.Store(opt)
}

func getFlowGraphRetryOpt() retry.Option {
	return fgRetryOptVal.Load().(retry.Option)
}
