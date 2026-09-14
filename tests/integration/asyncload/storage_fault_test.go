// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package asyncload

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestAsyncLoadStorageFaults(t *testing.T) {
	paramtable.Init()
	config := &paramtable.Get().MinioCfg
	scheme := "http"
	if config.UseSSL.GetAsBool() {
		scheme = "https"
	}
	backend := &url.URL{Scheme: scheme, Host: config.Address.GetValue()}
	proxy := &storageFaultProxy{
		forward: httputil.NewSingleHostReverseProxy(backend),
		bucket:  config.BucketName.GetValue(),
	}
	// NewSingleHostReverseProxy preserves Host, path, query and Range. S3
	// signatures remain valid while the TCP destination changes to MinIO.
	server := httptest.NewServer(proxy)
	t.Cleanup(server.Close)
	s := &loadSuite{async: true, faults: proxy}
	s.WithMilvusConfig("minio.address", strings.TrimPrefix(server.URL, "http://"))
	s.WithMilvusConfig("minio.useSSL", "false")
	suite.Run(t, s)
}

// The proxy only intercepts GETs for one immutable object after buildFixture
// has finished. Other objects and all build/write requests pass through.
type storageFaultProxy struct {
	forward http.Handler
	bucket  string
	mu      sync.Mutex
	fault   *storageFault
}

type storageFault struct {
	path       string
	failures   int
	injected   int
	reads      int
	entered    chan struct{}
	resume     chan struct{}
	resumeOnce sync.Once
}

func (p *storageFaultProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	f := p.fault
	if f == nil || r.Method != http.MethodGet || r.URL.Path != f.path {
		p.mu.Unlock()
		p.forward.ServeHTTP(w, r)
		return
	}
	f.reads++
	if f.reads == 1 {
		close(f.entered)
	}
	fail := f.failures > 0
	if fail {
		f.failures--
		f.injected++
	}
	p.mu.Unlock()
	if fail {
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("x-amz-request-id", "async-load-test")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`<Error><Code>SlowDown</Code><Message>injected by async load test</Message><RequestId>async-load-test</RequestId></Error>`))
		return
	}
	if f.resume != nil {
		select {
		case <-f.resume:
		case <-r.Context().Done():
			return
		}
	}
	p.forward.ServeHTTP(w, r)
}

func (f *storageFault) unblock() {
	if f.resume != nil {
		f.resumeOnce.Do(func() { close(f.resume) })
	}
}

// Installing a rule never changes an in-flight handler's rule. Cleanup always
// releases paused requests before dropping the collection or closing the proxy.
func (p *storageFaultProxy) arm(t *testing.T, object string, failures int, pause bool) *storageFault {
	t.Helper()
	f := &storageFault{
		path:     "/" + p.bucket + "/" + strings.TrimPrefix(object, "/"),
		failures: failures,
		entered:  make(chan struct{}),
	}
	if pause {
		f.resume = make(chan struct{})
	}
	p.mu.Lock()
	p.fault = f
	p.mu.Unlock()
	t.Cleanup(func() {
		f.unblock()
		p.mu.Lock()
		if p.fault == f {
			p.fault = nil
		}
		p.mu.Unlock()
	})
	return f
}

// Exercise the real network/SDK path and subsequent query results. Exact
// admission lifetimes and executor routing remain covered by the C++ tests.
func (s *loadSuite) checkStorageFaults(f loadFixture) {
	object := f.objects[0]
	if f.json {
		object = ""
		for _, candidate := range f.objects {
			if strings.HasSuffix(candidate, "/meta.json") {
				object = candidate
				break
			}
		}
	}
	s.Require().NotEmpty(object)
	s.T().Logf("injecting storage faults into %s", object)
	if !s.Run("SlowDown", func() {
		fault := s.faults.arm(s.T(), object, 2, false)
		s.loadAndQuery(f)
		s.faults.mu.Lock()
		injected, reads := fault.injected, fault.reads
		s.faults.mu.Unlock()
		s.Require().Equal(2, injected, "both S3 error responses must reach the real client")
		s.Require().Greater(reads, injected, "a successful retry must reach MinIO")
		s.release(f)
	}) {
		return
	}
	s.Run("ReleaseDuringRead", func() {
		fault := s.faults.arm(s.T(), object, 0, true)
		ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 2*time.Minute)
		defer cancel()
		status, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: f.name})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		select {
		case <-fault.entered:
		case <-ctx.Done():
			s.T().Fatal("load did not issue the selected object GET before timeout")
		}
		// Client-side LoadCollection cancellation does not cancel a background
		// load. Release exercises server-side load teardown instead. Issued
		// reads can finish before the worker observes cancellation; this case
		// verifies release/reload, not the C++ cancellation observation point.
		released := make(chan error, 1)
		go func() {
			status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: f.name})
			released <- merr.CheckRPCCall(status, err)
		}()
		// Observe server-side removal before allowing the issued GET to drain.
		// Release itself can wait for worker cleanup, so do not wait for its
		// completion while the storage response is deliberately paused.
		s.Require().Eventually(func() bool {
			state, err := s.Cluster.MilvusClient.GetLoadState(ctx, &milvuspb.GetLoadStateRequest{CollectionName: f.name})
			return merr.CheckRPCCall(state, err) == nil && state.GetState() == commonpb.LoadState_LoadStateNotLoad
		}, time.Minute, 100*time.Millisecond)
		fault.unblock()
		select {
		case err := <-released:
			s.Require().NoError(err)
		case <-ctx.Done():
			s.T().Fatal("collection release did not finish after the GET resumed")
		}
		s.CheckCollectionCacheReleased(f.collectionID)
		s.faults.mu.Lock()
		readsBeforeReload := fault.reads
		s.faults.mu.Unlock()
		s.loadAndQuery(f)
		s.faults.mu.Lock()
		readsAfterReload := fault.reads
		s.faults.mu.Unlock()
		s.Require().Greater(readsAfterReload, readsBeforeReload, "reload must read the object again")
		s.release(f)
	})
}
