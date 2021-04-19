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

package proxyservice

import (
	"testing"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"go.uber.org/zap"
)

func TestNaiveNodeIDAllocator_AllocOne(t *testing.T) {
	allocator := newNodeIDAllocator()

	num := 10
	for i := 0; i < num; i++ {
		nodeID := allocator.AllocOne()
		log.Debug("TestNaiveNodeIDAllocator_AllocOne", zap.Any("node id", nodeID))
	}
}
