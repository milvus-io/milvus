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

package schemaevolution

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// InstallGate coordinates collection-scoped query topology admission with a
// schema-changing broadcast. Implementations must keep Complete and Abort
// idempotent because broadcaster ACK callbacks are retried.
type InstallGate interface {
	PrepareSchemaInstall(ctx context.Context, collectionID int64) error
	CompleteSchemaInstall(ctx context.Context, collectionID int64, schema *schemapb.CollectionSchema, schemaBarrierTs uint64) error
	AbortSchemaInstall(ctx context.Context, collectionID int64)
}

type bypassKey struct{}

// WithAdmissionBypass marks target-schema work driven by the schema ACK
// callback. It is allowed to execute while normal topology admission is closed;
// the callback itself remains responsible for keeping the install gate closed
// until that work finishes.
func WithAdmissionBypass(ctx context.Context) context.Context {
	return context.WithValue(ctx, bypassKey{}, struct{}{})
}

func HasAdmissionBypass(ctx context.Context) bool {
	return ctx != nil && ctx.Value(bypassKey{}) != nil
}

type collectionGate struct {
	closed bool
	active int
	idle   chan struct{}
}

// GateManager is the in-memory QueryCoord admission and in-flight lease
// tracker. Durable recovery is owned by RootCoord/broadcaster; recovered
// schema broadcasts close this manager again before schedulers start.
type GateManager struct {
	mu          sync.Mutex
	collections map[int64]*collectionGate
}

func NewGateManager() *GateManager {
	return &GateManager{
		collections: make(map[int64]*collectionGate),
	}
}

func (m *GateManager) getOrCreate(collectionID int64) *collectionGate {
	gate, ok := m.collections[collectionID]
	if !ok {
		gate = &collectionGate{}
		m.collections[collectionID] = gate
	}
	return gate
}

// Acquire admits one topology operation and returns a lease release function.
// Admission and Close are serialized by the same mutex, so either the
// operation is counted by a subsequent drain or it observes the closed gate.
func (m *GateManager) Acquire(ctx context.Context, collectionID int64) (func(), error) {
	if HasAdmissionBypass(ctx) {
		return func() {}, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	gate := m.getOrCreate(collectionID)
	if gate.closed {
		return nil, merr.WrapErrServiceNotReadyMsg(
			"schema installation is in progress for collection %d", collectionID)
	}
	if gate.active == 0 {
		gate.idle = make(chan struct{})
	}
	gate.active++

	var once sync.Once
	return func() {
		once.Do(func() {
			m.release(collectionID)
		})
	}, nil
}

func (m *GateManager) release(collectionID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	gate, ok := m.collections[collectionID]
	if !ok || gate.active == 0 {
		return
	}
	gate.active--
	if gate.active == 0 {
		close(gate.idle)
		gate.idle = nil
		if !gate.closed {
			delete(m.collections, collectionID)
		}
	}
}

// Close rejects new topology operations. Existing leases remain valid and are
// drained by WaitIdle.
func (m *GateManager) Close(collectionID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getOrCreate(collectionID).closed = true
}

// WaitIdle waits until every topology operation admitted before Close has
// reached its terminal side-effect boundary.
func (m *GateManager) WaitIdle(ctx context.Context, collectionID int64) error {
	for {
		m.mu.Lock()
		gate := m.getOrCreate(collectionID)
		if gate.active == 0 {
			m.mu.Unlock()
			return nil
		}
		idle := gate.idle
		m.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-idle:
		}
	}
}

// Open releases admission. It is idempotent for ACK callback retry and
// pre-cut abort paths.
func (m *GateManager) Open(collectionID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	gate, ok := m.collections[collectionID]
	if !ok {
		return
	}
	gate.closed = false
	if gate.active == 0 {
		delete(m.collections, collectionID)
	}
}

func (m *GateManager) IsClosed(collectionID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	gate, ok := m.collections[collectionID]
	return ok && gate.closed
}

func (m *GateManager) Active(collectionID int64) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	gate, ok := m.collections[collectionID]
	if !ok {
		return 0
	}
	return gate.active
}

func (m *GateManager) Check(collectionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	gate, ok := m.collections[collectionID]
	if ok && gate.closed {
		return merr.WrapErrServiceNotReadyMsg(
			"schema installation is in progress for collection %d", collectionID)
	}
	return nil
}
