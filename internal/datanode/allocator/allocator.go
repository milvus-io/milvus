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

package allocator

import (
	"context"

	gAllocator "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type Allocator interface {
	Start() error
	Close()
	AllocOne() (UniqueID, error)
	Alloc(count uint32) (UniqueID, UniqueID, error)
	GetGenerator(count int, done <-chan struct{}) (<-chan UniqueID, error)
	GetIDAlloactor() *gAllocator.IDAllocator
}

var _ Allocator = (*Impl)(nil)

type Impl struct {
	// Start() error
	// Close() error
	// AllocOne() (UniqueID, error)
	// Alloc(count uint32) (UniqueID, UniqueID, error)
	*gAllocator.IDAllocator
}

func New(ctx context.Context, rootCoord types.RootCoordClient, peerID UniqueID) (Allocator, error) {
	idAlloc, err := gAllocator.NewIDAllocator(ctx, rootCoord, peerID)
	if err != nil {
		return nil, err
	}
	return &Impl{idAlloc}, nil
}

func (a *Impl) GetIDAlloactor() *gAllocator.IDAllocator {
	return a.IDAllocator
}

func (a *Impl) GetGenerator(count int, done <-chan struct{}) (<-chan UniqueID, error) {
	idStart, _, err := a.Alloc(uint32(count))
	if err != nil {
		return nil, err
	}

	rt := make(chan UniqueID)
	go func(rt chan<- UniqueID) {
		for i := 0; i < count; i++ {
			select {
			case <-done:
				close(rt)
				return
			case rt <- idStart + UniqueID(i):
			}
		}
		close(rt)
	}(rt)

	return rt, nil
}
