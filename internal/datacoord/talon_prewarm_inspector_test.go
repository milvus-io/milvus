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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type recordingTalonPrewarm struct {
	stats        map[string]int
	batches      [][]TalonPrewarmRequest
	statFailure  string
	batchFailure bool
	pending      chan struct{}
	closeStarted chan struct{}
	closeRelease chan struct{}
	closes       int
	mu           sync.Mutex
}

func (c *recordingTalonPrewarm) Stat(ctx context.Context, uri string) (uint64, string, error) {
	c.stats[uri]++
	if uri == c.statFailure {
		return 0, "", errors.New("stat unavailable")
	}
	return uint64(len(uri)), "etag:" + uri, nil
}

func (c *recordingTalonPrewarm) BatchLoad(ctx context.Context, requests []TalonPrewarmRequest) error {
	c.batches = append(c.batches, append([]TalonPrewarmRequest(nil), requests...))
	if c.pending != nil {
		close(c.pending)
		<-ctx.Done()
		return ctx.Err()
	}
	if c.batchFailure {
		return errors.New("worker unavailable")
	}
	return nil
}

func (c *recordingTalonPrewarm) Close() {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	if c.closeStarted != nil {
		close(c.closeStarted)
	}
	if c.closeRelease != nil {
		<-c.closeRelease
	}
}

func TestTalonPrewarmInspectorRounds(t *testing.T) {
	ctx := context.Background()
	descriptors := []string{"collection1/v1", "collection2/v1", "collection1/v1", "collection2/v2", "bad"}
	var supplierErr error
	c := &recordingTalonPrewarm{stats: map[string]int{}, statFailure: "s3://bad", batchFailure: true}
	inspector := NewTalonPrewarmInspector(ctx, func(context.Context) ([]string, error) { return descriptors, supplierErr }, c,
		func(s string) (string, error) { return "s3://" + s, nil }, time.Hour, 2)
	inspector.inspect(ctx)
	require.Len(t, c.batches, 2) // Batch failure must not suppress the second batch.
	require.Len(t, c.batches[0], 2)
	require.Len(t, c.batches[1], 1)
	require.Len(t, c.stats, 4)
	require.Len(t, inspector.metadata, 3)
	for _, batch := range c.batches {
		for _, r := range batch {
			require.Equal(t, uint64(len(r.URI)), r.Size)
			require.Equal(t, "etag:"+r.URI, r.Version)
		}
	}
	c.batchFailure = false
	inspector.inspect(ctx)
	require.Len(t, c.batches, 4) // Cached metadata never means permanently warm.
	require.Equal(t, 1, c.stats["s3://collection1/v1"])
	require.Equal(t, 2, c.stats["s3://bad"])
	inspector.inspect(ctx) // Previous loads succeeded, but active objects must load again.
	require.Len(t, c.batches, 6)
	require.Equal(t, 1, c.stats["s3://collection1/v1"])
	supplierErr = errors.New("datacoord recovering")
	descriptors = nil
	inspector.inspect(ctx)
	require.Len(t, inspector.metadata, 3)
	require.Len(t, c.batches, 6)
	supplierErr = nil
	descriptors = []string{"collection1/v2", "collection2/v2"}
	inspector.inspect(ctx)
	require.Len(t, inspector.metadata, 2)
	require.NotContains(t, inspector.metadata, "s3://collection1/v1")
	require.Equal(t, 1, c.stats["s3://collection1/v2"])
	require.Equal(t, 1, c.stats["s3://collection2/v2"])
	descriptors = nil
	inspector.inspect(ctx)
	require.Empty(t, inspector.metadata)
}

func TestTalonPrewarmInspectorStopPending(t *testing.T) {
	c := &recordingTalonPrewarm{stats: map[string]int{}, pending: make(chan struct{}), closeStarted: make(chan struct{}), closeRelease: make(chan struct{})}
	inspector := NewTalonPrewarmInspector(context.Background(), func(context.Context) ([]string, error) { return []string{"one", "two"}, nil }, c, func(s string) (string, error) { return s, nil }, time.Hour, 1)
	inspector.Start()
	<-c.pending
	stopped := make(chan struct{})
	go func() { inspector.Stop(); inspector.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop blocked on native drain")
	}
	<-c.closeStarted
	require.Len(t, c.batches, 1)
	inspector.Start()
	c.mu.Lock()
	require.Equal(t, 1, c.closes)
	c.mu.Unlock()
	close(c.closeRelease)
}

func TestTalonPrewarmInspectorStopBeforeStart(t *testing.T) {
	c := &recordingTalonPrewarm{closeStarted: make(chan struct{})}
	inspector := NewTalonPrewarmInspector(context.Background(), nil, c, nil, time.Hour, 1)
	inspector.Stop()
	inspector.Start()
	inspector.Stop()
	<-c.closeStarted
	c.mu.Lock()
	require.Equal(t, 1, c.closes)
	c.mu.Unlock()
}

func TestTalonPrewarmInspectorResolutionFailure(t *testing.T) {
	c := &recordingTalonPrewarm{stats: map[string]int{}}
	inspector := NewTalonPrewarmInspector(context.Background(), func(context.Context) ([]string, error) { return []string{"invalid", "valid", "valid"}, nil }, c, func(s string) (string, error) {
		if s == "invalid" {
			return "", errors.New("malformed metadata")
		}
		return s, nil
	}, time.Hour, 2)
	inspector.inspect(context.Background())
	require.Len(t, c.batches, 1)
	require.Equal(t, []TalonPrewarmRequest{{URI: "valid", Size: 5, Version: "etag:valid"}}, c.batches[0])
}

func TestTalonPrewarmInspectorStopBlockedSupplier(t *testing.T) {
	supplierEntered := make(chan struct{})
	releaseSupplier := make(chan struct{})
	var releaseOnce sync.Once
	c := &recordingTalonPrewarm{stats: map[string]int{}, closeStarted: make(chan struct{})}
	inspector := NewTalonPrewarmInspector(context.Background(), func(context.Context) ([]string, error) {
		close(supplierEntered)
		// Model the uncancellable segMu.RLock wait behind a catalog writer.
		<-releaseSupplier
		return []string{"one", "two"}, nil
	}, c, func(s string) (string, error) { return s, nil }, time.Hour, 1)
	stopped := make(chan struct{})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseSupplier) })
		<-stopped
		<-c.closeStarted
	})
	inspector.Start()
	<-supplierEntered
	go func() { inspector.Stop(); inspector.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop blocked on uncancellable prewarm supplier")
	}
	select {
	case <-c.closeStarted:
		t.Fatal("client closed before scheduling loop finished")
	default:
	}
	releaseOnce.Do(func() { close(releaseSupplier) })
	select {
	case <-c.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("client cleanup did not follow supplier completion")
	}
	require.Empty(t, c.stats)
	require.Empty(t, c.batches)
	inspector.Stop()
	c.mu.Lock()
	require.Equal(t, 1, c.closes)
	c.mu.Unlock()
}
