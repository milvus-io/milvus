// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package registry

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// AppendFirstAppliedResolver answers, for one message type, whether the
// append-first copies of a broadcast on vchannels were already applied on this
// cluster, and at which time tick they were acknowledged.
//
// The broadcaster asks it only when it holds no task for the broadcast. A task
// is collected some time after its ack callback has run, and a copy of the
// broadcast can be delivered again after that. The effect the callback
// recorded is then the only durable fact left that the append-first copies
// were acknowledged here.
type AppendFirstAppliedResolver func(ctx context.Context, vchannels []string) (timeTick uint64, applied bool, err error)

var (
	appendFirstAppliedResolversMu sync.RWMutex
	appendFirstAppliedResolvers   = map[message.MessageType]AppendFirstAppliedResolver{}
)

// RegisterAppendFirstAppliedResolver sets the resolver of a message type.
func RegisterAppendFirstAppliedResolver(typ message.MessageType, resolver AppendFirstAppliedResolver) {
	appendFirstAppliedResolversMu.Lock()
	defer appendFirstAppliedResolversMu.Unlock()
	appendFirstAppliedResolvers[typ] = resolver
}

// ResolveAppendFirstApplied asks the resolver of a message type. Without a
// resolver the answer is "not applied".
func ResolveAppendFirstApplied(ctx context.Context, typ message.MessageType, vchannels []string) (uint64, bool, error) {
	appendFirstAppliedResolversMu.RLock()
	resolver := appendFirstAppliedResolvers[typ]
	appendFirstAppliedResolversMu.RUnlock()
	if resolver == nil {
		return 0, false, nil
	}
	return resolver(ctx, vchannels)
}

func resetAppendFirstAppliedResolvers() {
	appendFirstAppliedResolversMu.Lock()
	defer appendFirstAppliedResolversMu.Unlock()
	appendFirstAppliedResolvers = map[message.MessageType]AppendFirstAppliedResolver{}
}
