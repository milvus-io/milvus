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

// Package failpoint provides WAL fault boundaries for the integration fixture.
// Callers use standard PingCAP markers, rewritten only in the test fixture.
package failpoint

const (
	BeforeIdempotencyAppend = "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/idempotency/beforeIdempotencyAppend"
	AfterIdempotencyAppend  = "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/idempotency/afterIdempotencyAppend"
	AfterBackendAppend      = "github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor/afterBackendAppend"
)

// Config selects one request and, optionally, one shard. Each point fires once
// per vchannel in Directory, even if the owning process is killed and replaced.
type Config struct {
	PID         int    `json:"pid"`
	Key         string `json:"key"`
	VChannel    string `json:"vchannel"`
	MessageType string `json:"message_type"`
	Action      string `json:"action"` // observe, pause, or error
	Directory   string `json:"directory"`
}

// Receipt is published by the actual write path, never by the test controller.
type Receipt struct {
	Key         string   `json:"key"`
	VChannel    string   `json:"vchannel"`
	MessageType string   `json:"message_type"`
	MessageID   string   `json:"message_id"`
	TimeTick    uint64   `json:"time_tick"`
	IDs         []int64  `json:"ids"`
	Offsets     []uint32 `json:"offsets"`
	PID         int      `json:"pid"`
}
