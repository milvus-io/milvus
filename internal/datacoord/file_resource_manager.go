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

// Package datacoord contains core functions in datacoord
package datacoord

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"go.uber.org/zap"
)

type FileResourceManager struct {
	lock.RWMutex
	// currnet resource
	meta      meta
	resources []*internalpb.FileResourceInfo
	version   uint64

	// node version
	nodeManager  session.NodeManager
	distribution map[int64]uint64

	notifyCh chan struct{}
	sf       conc.Singleflight[any]
}

func (m *FileResourceManager) syncLoop() {
	ctx := context.Background()

	for {
		select {
		case <-m.notifyCh:
			err := m.sync(ctx)
			if err != nil {
				// retry if error exist
				m.sf.Do("retry", func() (any, error) {
					time.Sleep(5 * time.Second)
					m.Notify()
					return nil, nil
				})
			}
		}
	}
}

func (m *FileResourceManager) Notify() {
	select {
	case m.notifyCh <- struct{}{}:
	default:
	}
}

func (m *FileResourceManager) sync(ctx context.Context) error {
	nodes := m.nodeManager.GetClientIDs()

	var syncErr error

	newDistribution := make(map[int64]uint64)
	for _, node := range nodes {
		if m.distribution[node] != m.version {
			c, err := m.nodeManager.GetClient(node)
			if err != nil {
				log.Warn("sync file resource failed, fetch client failed", zap.Error(err))
				syncErr = err
				continue
			}
			status, err := c.SyncFileResource(ctx, &internalpb.SyncFileResourceRequest{
				Resources: m.resources,
			})

			if err != nil {
				log.Info("sync file resource failed", zap.Int64("nodeID", node), zap.Error(err))
				continue
			}

			if err = merr.Error(status); err != nil {
				log.Info("sync file resource failed", zap.Int64("nodeID", node), zap.Error(err))
				continue
			}

			newDistribution[node] = m.version
		} else {
			newDistribution[node] = m.version
		}
	}

	m.UpdateDistribution(newDistribution)

	if syncErr != nil {
		return syncErr
	}
	return nil
}

func (m *FileResourceManager) UpdateDistribution(new map[int64]uint64) {
	m.Lock()
	m.Unlock()
	m.distribution = new
}
