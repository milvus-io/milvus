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

package cluster

import (
	"fmt"
	"strconv"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// Manager is the interface for worker manager.
type Manager interface {
	GetWorker(nodeID int64) (Worker, error)
}

// WorkerBuilder is function alias to build a worker from NodeID
type WorkerBuilder func(nodeID int64) (Worker, error)

type grpcWorkerManager struct {
	workers *typeutil.ConcurrentMap[int64, Worker]
	builder WorkerBuilder
	sf      conc.Singleflight[Worker] //singleflight.Group
}

// GetWorker returns worker with specified nodeID.
func (m *grpcWorkerManager) GetWorker(nodeID int64) (Worker, error) {
	worker, ok := m.workers.Get(nodeID)
	var err error
	if !ok {
		worker, err, _ = m.sf.Do(strconv.FormatInt(nodeID, 10), func() (Worker, error) {
			worker, err = m.builder(nodeID)
			if err != nil {
				log.Warn("failed to build worker",
					zap.Int64("nodeID", nodeID),
					zap.Error(err),
				)
				return nil, err
			}
			old, exist := m.workers.GetOrInsert(nodeID, worker)
			if exist {
				worker.Stop()
				worker = old
			}
			return worker, nil
		})
		if err != nil {
			return nil, err
		}
	}
	if !worker.IsHealthy() {
		// TODO wrap error
		return nil, fmt.Errorf("node is not healthy: %d", nodeID)
	}
	return worker, nil
}

func NewWorkerManager(builder WorkerBuilder) Manager {
	return &grpcWorkerManager{
		workers: typeutil.NewConcurrentMap[int64, Worker](),
		builder: builder,
	}
}
