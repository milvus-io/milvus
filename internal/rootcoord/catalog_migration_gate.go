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

package rootcoord

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type catalogMigrationGate struct {
	mu       sync.Mutex
	draining bool
	inflight int
	drained  chan struct{}
}

func newCatalogMigrationGate() *catalogMigrationGate {
	return &catalogMigrationGate{drained: make(chan struct{})}
}

func (g *catalogMigrationGate) BeginMetadataWrite(ctx context.Context) (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.draining {
		return nil, merr.WrapErrServiceNotReadyMsg("rootcoord catalog migration is draining metadata writes")
	}
	g.inflight++
	return func() {
		g.finishMetadataWrite()
	}, nil
}

func (g *catalogMigrationGate) StartDraining() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.draining {
		return
	}
	g.draining = true
	g.drained = make(chan struct{})
	if g.inflight == 0 {
		close(g.drained)
	}
}

func (g *catalogMigrationGate) Resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.draining = false
	if g.inflight == 0 {
		select {
		case <-g.drained:
		default:
			close(g.drained)
		}
	}
}

func (g *catalogMigrationGate) WaitDrained(ctx context.Context) error {
	g.mu.Lock()
	drained := g.drained
	if !g.draining && g.inflight == 0 {
		g.mu.Unlock()
		return nil
	}
	g.mu.Unlock()

	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *catalogMigrationGate) finishMetadataWrite() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inflight > 0 {
		g.inflight--
	}
	if g.draining && g.inflight == 0 {
		select {
		case <-g.drained:
		default:
			close(g.drained)
		}
	}
}
