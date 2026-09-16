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

package l0materializer

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type materializeTask struct {
	materializer *L0Materializer
	target       uint64
	done         atomic.Bool
}

func (t *materializeTask) Execute(ctx context.Context) error {
	m := t.materializer
	m.materializeMu.Lock()
	defer m.materializeMu.Unlock()
	if t.done.Load() {
		return nil
	}
	if err := m.materialize(ctx, t.target); err != nil {
		return errors.Mark(err, nodescheduler.ErrDelay)
	}
	// Completion and the next scheduling decision share observation's lock.
	// A concurrent observation or L1-bound raise either feeds this continuation
	// or creates the next task itself; there is no completion wakeup gap.
	m.mu.Lock()
	t.done.Store(true)
	m.task = nil
	next := m.scheduleLocked()
	m.mu.Unlock()
	m.submit(next)
	return nil
}
