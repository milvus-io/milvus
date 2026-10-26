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

package datacoord

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// TalonPrewarmRequest names an object under the cache identity its readers
// use. An empty Version is Talon's unchecked version.
type TalonPrewarmRequest struct {
	URI     string
	Size    uint64
	Version string
}

// TalonPrewarmClient is the external Talon boundary. Cancellation stops waiting;
// Close must retain accepted native operations until their callbacks complete.
type TalonPrewarmClient interface {
	Stat(context.Context, string) (uint64, string, error)
	BatchLoad(context.Context, []TalonPrewarmRequest) error
	Close()
}

type TalonPrewarmInspector struct {
	ctx       context.Context
	cancel    context.CancelFunc
	supplier  func(context.Context) ([]string, error)
	client    TalonPrewarmClient
	resolve   func(string) (string, uint64, error)
	interval  time.Duration
	batchSize int
	// metadata keeps the stat results of objects whose descriptors record no size.
	metadata  map[string]TalonPrewarmRequest
	mu        sync.Mutex
	started   bool
	stopped   bool
	done      chan struct{}
	closeOnce sync.Once
}

func NewTalonPrewarmInspector(ctx context.Context, supplier func(context.Context) ([]string, error), client TalonPrewarmClient, resolve func(string) (string, uint64, error), interval time.Duration, batchSize int) *TalonPrewarmInspector {
	ctx, cancel := context.WithCancel(ctx) // #nosec G118 -- Stop owns cancellation for the inspector lifetime.
	return &TalonPrewarmInspector{ctx: ctx, cancel: cancel, supplier: supplier, client: client, resolve: resolve, interval: interval, batchSize: batchSize, metadata: make(map[string]TalonPrewarmRequest), done: make(chan struct{})}
}

func (i *TalonPrewarmInspector) Start() {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.started || i.stopped {
		return
	}
	i.started = true
	go func() {
		defer close(i.done)
		for i.ctx.Err() == nil {
			i.inspect(i.ctx)
			timer := time.NewTimer(i.interval)
			select {
			case <-i.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

func (i *TalonPrewarmInspector) Stop() {
	i.mu.Lock()
	if !i.stopped {
		i.stopped = true
		i.cancel()
		if !i.started {
			close(i.done)
		}
	}
	i.mu.Unlock()
	// A supplier can be waiting on DataCoord's uncancellable metadata lock.
	// Keep the loop and client alive in one owned cleanup without delaying Stop.
	// Native Close then drains accepted callbacks before releasing resources.
	i.closeOnce.Do(func() {
		go func() {
			<-i.done
			i.client.Close()
		}()
	})
}

func (i *TalonPrewarmInspector) inspect(ctx context.Context) {
	descriptors, err := i.supplier(ctx)
	if err != nil {
		mlog.RatedWarn(ctx, 1, "Talon prewarm enumeration failed", mlog.Err(err))
		return
	}
	active := make(map[string]uint64, len(descriptors))
	failures := 0
	for _, descriptor := range descriptors {
		if ctx.Err() != nil {
			return
		}
		uri, size, err := i.resolve(descriptor)
		if err != nil {
			failures++
			mlog.RatedWarn(ctx, 1, "Talon prewarm resolution failed", mlog.Err(err))
			continue
		}
		// Empty means a valid, not-yet-published manifest descriptor.
		if uri == "" {
			continue
		}
		active[uri] = size
	}
	for uri := range i.metadata {
		if size, ok := active[uri]; !ok || size > 0 {
			delete(i.metadata, uri)
		}
	}
	batch := make([]TalonPrewarmRequest, 0, i.batchSize)
	loaded := 0
	flush := func() {
		if len(batch) == 0 || ctx.Err() != nil {
			return
		}
		if err := i.client.BatchLoad(ctx, batch); err != nil {
			failures++
			mlog.RatedWarn(ctx, 1, "Talon prewarm batch load failed", mlog.Int("count", len(batch)), mlog.Err(err))
		} else {
			loaded += len(batch)
		}
		batch = batch[:0]
	}
	add := func(request TalonPrewarmRequest) {
		batch = append(batch, request)
		if len(batch) == i.batchSize {
			flush()
		}
	}
	// Load the identity milvus-storage readers use (Manifest::ReadFrom,
	// TalonFileSystem::OpenTalon): a recorded manifest size skips the HEAD and
	// opens Talon's unchecked version; any other manifest opens with the ETag
	// that stat reports. Manifests that record their size load first, since any
	// other may wait for a serial stat.
	for uri, size := range active {
		if ctx.Err() != nil {
			return
		}
		if size > 0 {
			add(TalonPrewarmRequest{URI: uri, Size: size})
		}
	}
	flush()
	for uri, size := range active {
		if ctx.Err() != nil {
			return
		}
		if size > 0 {
			continue
		}
		request, ok := i.metadata[uri]
		if !ok {
			size, version, err := i.client.Stat(ctx, uri)
			if err != nil {
				failures++
				mlog.RatedWarn(ctx, 1, "Talon prewarm stat failed", mlog.String("uri", uri), mlog.Err(err))
				continue
			}
			request = TalonPrewarmRequest{URI: uri, Size: size, Version: version}
			i.metadata[uri] = request
		}
		add(request)
	}
	flush()
	mlog.Info(ctx, "Talon prewarm round finished", mlog.Int("active", len(active)), mlog.Int("loaded", loaded), mlog.Int("failures", failures))
}
