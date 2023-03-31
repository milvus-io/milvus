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

package tsafe

import (
	"go.uber.org/atomic"

	. "github.com/milvus-io/milvus/pkg/util/typeutil"
)

type tSafe struct {
	channel string
	tSafe   atomic.Uint64
	closed  atomic.Bool
}

func (ts *tSafe) valid() bool {
	return !ts.closed.Load()
}

func (ts *tSafe) close() {
	ts.closed.Store(true)
}

func (ts *tSafe) get() Timestamp {
	return ts.tSafe.Load()
}

func (ts *tSafe) set(t Timestamp) {
	ts.tSafe.Store(t)
}

func newTSafe(channel string, timestamp uint64) *tSafe {
	ts := &tSafe{
		channel: channel,
	}
	ts.tSafe.Store(timestamp)
	ts.closed.Store(false)

	return ts
}
