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

package querynode

import (
	"context"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/types"
)

type QueryNode struct {
	ctx    context.Context
	cancel context.CancelFunc

	NodeID    UniqueID
	stateCode atomic.Value

	historical *Historical
	streaming  *Streaming

	// clients
	masterService types.MasterService
	queryService  types.QueryService
	indexService  types.IndexService
	dataService   types.DataService

	msFactory msgstream.Factory
	scheduler *taskScheduler
}
