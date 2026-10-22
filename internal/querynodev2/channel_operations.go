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

package querynodev2

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

// channelOperations serializes each channel's Watch/Unsub lifecycle, including
// initialization rollback and distribution publication. Different channels can
// proceed independently, and queued RPCs can stop waiting when canceled.
type channelOperations struct {
	mu       sync.Mutex
	channels map[string]*channelOperation
}

type channelOperation struct {
	semaphore *semaphore.Weighted
	refs      int // Includes both the owner and queued callers.
}

func newChannelOperations() *channelOperations {
	return &channelOperations{channels: make(map[string]*channelOperation)}
}

func (o *channelOperations) acquire(ctx context.Context, channel string) (func(), error) {
	o.mu.Lock()
	operation := o.channels[channel]
	if operation == nil {
		operation = &channelOperation{semaphore: semaphore.NewWeighted(1)}
		o.channels[channel] = operation
	}
	operation.refs++
	o.mu.Unlock()

	unref := func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		operation.refs--
		if operation.refs == 0 {
			delete(o.channels, channel)
		}
	}
	if err := operation.semaphore.Acquire(ctx, 1); err != nil {
		unref()
		return nil, err
	}
	return func() {
		operation.semaphore.Release(1)
		unref()
	}, nil
}
