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

package proxy

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ---------------------------------------------------------------------------
// The extension under test.
// ---------------------------------------------------------------------------

// loadSeamExtension takes over the load-semantics group the way a form that
// makes a collection serviceable on demand does: it answers the load, release
// and progress RPCs itself and lets a refresh through, because a refresh is not
// a load.
//
// It records the very request pointer it was handed for each method, which is
// what lets the tests below assert the seam forwards the caller's own request
// rather than a copy of it.
type loadSeamExtension struct {
	extension.NoopProxyExtension

	loadCollectionStatus    *commonpb.Status
	releaseCollectionStatus *commonpb.Status
	loadPartitionsStatus    *commonpb.Status
	releasePartitionsStatus *commonpb.Status
	loadStateResponse       *milvuspb.GetLoadStateResponse
	loadingProgressResponse *milvuspb.GetLoadingProgressResponse

	// consulted counts every consultation of every method in the group, so a
	// test can assert a seam was never reached at all.
	consulted int

	sawLoadCollection    *milvuspb.LoadCollectionRequest
	sawReleaseCollection *milvuspb.ReleaseCollectionRequest
	sawLoadPartitions    *milvuspb.LoadPartitionsRequest
	sawReleasePartitions *milvuspb.ReleasePartitionsRequest
	sawLoadState         *milvuspb.GetLoadStateRequest
	sawLoadingProgress   *milvuspb.GetLoadingProgressRequest
}

func (e *loadSeamExtension) InterceptLoadCollection(_ context.Context, req *milvuspb.LoadCollectionRequest) *commonpb.Status {
	e.consulted++
	e.sawLoadCollection = req
	if req.GetRefresh() {
		return nil
	}
	return e.loadCollectionStatus
}

func (e *loadSeamExtension) InterceptReleaseCollection(_ context.Context, req *milvuspb.ReleaseCollectionRequest) *commonpb.Status {
	e.consulted++
	e.sawReleaseCollection = req
	return e.releaseCollectionStatus
}

func (e *loadSeamExtension) InterceptLoadPartitions(_ context.Context, req *milvuspb.LoadPartitionsRequest) *commonpb.Status {
	e.consulted++
	e.sawLoadPartitions = req
	if req.GetRefresh() {
		return nil
	}
	return e.loadPartitionsStatus
}

func (e *loadSeamExtension) InterceptReleasePartitions(_ context.Context, req *milvuspb.ReleasePartitionsRequest) *commonpb.Status {
	e.consulted++
	e.sawReleasePartitions = req
	return e.releasePartitionsStatus
}

func (e *loadSeamExtension) InterceptGetLoadState(_ context.Context, req *milvuspb.GetLoadStateRequest) *milvuspb.GetLoadStateResponse {
	e.consulted++
	e.sawLoadState = req
	return e.loadStateResponse
}

func (e *loadSeamExtension) InterceptGetLoadingProgress(_ context.Context, req *milvuspb.GetLoadingProgressRequest) *milvuspb.GetLoadingProgressResponse {
	e.consulted++
	e.sawLoadingProgress = req
	return e.loadingProgressResponse
}

// installLoadSeamExtension installs an extension that takes over the whole
// group. Each canned answer is a distinct value the tests identity-compare
// against, so an assertion cannot pass on a status that merely looks the same.
func installLoadSeamExtension(t *testing.T) *loadSeamExtension {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	ext := &loadSeamExtension{
		loadCollectionStatus:    merr.Success(),
		releaseCollectionStatus: merr.Success(),
		loadPartitionsStatus:    merr.Success(),
		releasePartitionsStatus: merr.Success(),
		loadStateResponse: &milvuspb.GetLoadStateResponse{
			Status: merr.Success(),
			State:  commonpb.LoadState_LoadStateLoaded,
		},
		loadingProgressResponse: &milvuspb.GetLoadingProgressResponse{
			Status:   merr.Success(),
			Progress: 100,
		},
	}
	require.NoError(t, extension.SetProvider(testProvider{
		caps: extension.Capabilities{ProxyExt: ext},
	}))
	return ext
}

// errEnqueueRefused is what a mocked task queue answers with. Its appearance in
// a response is the proof that the handler went past the seam and into the
// native path: nothing else in these tests can produce it.
var errEnqueueRefused = errors.New("task queue refused the task")

// countDDEnqueue replaces the DDL task queue's Enqueue with a refusal and
// returns a pointer to the number of times it ran.
//
// The count is the load-bearing half. "The native path ran" and "the native
// path did not run" are both asserted on it, so a seam that stopped
// short-circuiting, or one that short-circuited when it must not, moves a
// number rather than changing an error message.
func countDDEnqueue(t *testing.T) *int {
	t.Helper()
	calls := 0
	mock := mockey.Mock((*ddTaskQueue).Enqueue).To(func(*ddTaskQueue, task) error {
		calls++
		return errEnqueueRefused
	}).Build()
	t.Cleanup(func() { mock.UnPatch() })
	return &calls
}

// newLoadSeamProxy builds a healthy proxy whose DDL queue is the mocked one.
func newLoadSeamProxy() *Proxy {
	node := &Proxy{sched: &taskScheduler{ddQueue: &ddTaskQueue{}}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	return node
}

// ---------------------------------------------------------------------------
// Inertness: with no provider installed a seam does nothing at all.
// ---------------------------------------------------------------------------

// TestLoadSemanticsSeamsAreInertWithNoProviderInstalled is the per-seam
// inertness proof. Each seam must answer nil - "fall through" - and must do it
// without allocating, because with no provider installed the whole group is one
// atomic load and one nil comparison per RPC.
//
// The allocation count is not decoration: it is what fails if a seam is ever
// rewritten to build a request wrapper, log line or default response before
// finding out that nobody is listening. It measures heap allocations, so a
// temporary the compiler proves does not escape costs nothing and does not move
// it - which is the right answer, since such a temporary costs a stock binary
// nothing either.
func TestLoadSemanticsSeamsAreInertWithNoProviderInstalled(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	ctx := context.Background()
	loadCollection := &milvuspb.LoadCollectionRequest{CollectionName: "coll"}
	releaseCollection := &milvuspb.ReleaseCollectionRequest{CollectionName: "coll"}
	loadPartitions := &milvuspb.LoadPartitionsRequest{CollectionName: "coll"}
	releasePartitions := &milvuspb.ReleasePartitionsRequest{CollectionName: "coll"}
	loadState := &milvuspb.GetLoadStateRequest{CollectionName: "coll"}
	loadingProgress := &milvuspb.GetLoadingProgressRequest{CollectionName: "coll"}

	assert.Nil(t, interceptLoadCollection(ctx, loadCollection))
	assert.Nil(t, interceptReleaseCollection(ctx, releaseCollection))
	assert.Nil(t, interceptLoadPartitions(ctx, loadPartitions))
	assert.Nil(t, interceptReleasePartitions(ctx, releasePartitions))
	assert.Nil(t, interceptGetLoadState(ctx, loadState))
	assert.Nil(t, interceptGetLoadingProgress(ctx, loadingProgress))

	for name, seam := range map[string]func(){
		"LoadCollection":     func() { interceptLoadCollection(ctx, loadCollection) },
		"ReleaseCollection":  func() { interceptReleaseCollection(ctx, releaseCollection) },
		"LoadPartitions":     func() { interceptLoadPartitions(ctx, loadPartitions) },
		"ReleasePartitions":  func() { interceptReleasePartitions(ctx, releasePartitions) },
		"GetLoadState":       func() { interceptGetLoadState(ctx, loadState) },
		"GetLoadingProgress": func() { interceptGetLoadingProgress(ctx, loadingProgress) },
	} {
		assert.Zero(t, testing.AllocsPerRun(100, seam),
			"the %s seam must not allocate with no provider installed", name)
	}
}

// TestLoadSemanticsSeamsForwardTheCallersOwnRequest pins what an installed
// extension is handed: the request the client sent, not a copy of it. A seam
// that rebuilt the request would hide later fields from every implementation
// and cost a stock-shaped allocation on a control-plane RPC.
func TestLoadSemanticsSeamsForwardTheCallersOwnRequest(t *testing.T) {
	ext := installLoadSeamExtension(t)
	ctx := context.Background()

	loadCollection := &milvuspb.LoadCollectionRequest{CollectionName: "coll"}
	releaseCollection := &milvuspb.ReleaseCollectionRequest{CollectionName: "coll"}
	loadPartitions := &milvuspb.LoadPartitionsRequest{CollectionName: "coll"}
	releasePartitions := &milvuspb.ReleasePartitionsRequest{CollectionName: "coll"}
	loadState := &milvuspb.GetLoadStateRequest{CollectionName: "coll"}
	loadingProgress := &milvuspb.GetLoadingProgressRequest{CollectionName: "coll"}

	assert.Same(t, ext.loadCollectionStatus, interceptLoadCollection(ctx, loadCollection))
	assert.Same(t, ext.releaseCollectionStatus, interceptReleaseCollection(ctx, releaseCollection))
	assert.Same(t, ext.loadPartitionsStatus, interceptLoadPartitions(ctx, loadPartitions))
	assert.Same(t, ext.releasePartitionsStatus, interceptReleasePartitions(ctx, releasePartitions))
	assert.Same(t, ext.loadStateResponse, interceptGetLoadState(ctx, loadState))
	assert.Same(t, ext.loadingProgressResponse, interceptGetLoadingProgress(ctx, loadingProgress))

	assert.Same(t, loadCollection, ext.sawLoadCollection)
	assert.Same(t, releaseCollection, ext.sawReleaseCollection)
	assert.Same(t, loadPartitions, ext.sawLoadPartitions)
	assert.Same(t, releasePartitions, ext.sawReleasePartitions)
	assert.Same(t, loadState, ext.sawLoadState)
	assert.Same(t, loadingProgress, ext.sawLoadingProgress)
}

// ---------------------------------------------------------------------------
// The four status entry points.
// ---------------------------------------------------------------------------

func TestLoadCollectionIsAnsweredByTheSeam(t *testing.T) {
	ext := installLoadSeamExtension(t)
	enqueued := countDDEnqueue(t)
	node := newLoadSeamProxy()

	request := &milvuspb.LoadCollectionRequest{DbName: "db", CollectionName: "coll"}
	status, err := node.LoadCollection(context.Background(), request)

	require.NoError(t, err)
	assert.Same(t, ext.loadCollectionStatus, status,
		"the handler must return the extension's status as it stands, not a status of its own that happens to agree")
	assert.Zero(t, *enqueued, "a load the extension answered must never reach the task queue")
	assert.Same(t, request, ext.sawLoadCollection,
		"LoadCollection must consult the seam with the request it was given")
}

func TestLoadCollectionReachesTheNativePathWithNoProviderInstalled(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	enqueued := countDDEnqueue(t)
	node := newLoadSeamProxy()

	status, err := node.LoadCollection(context.Background(), &milvuspb.LoadCollectionRequest{
		DbName:         "db",
		CollectionName: "coll",
	})

	require.NoError(t, err)
	assert.Equal(t, 1, *enqueued, "with no provider installed the load must reach the native task path")
	assert.Equal(t, errEnqueueRefused.Error(), status.GetReason(),
		"the native path's own answer must reach the client unchanged")
}

func TestReleaseCollectionIsAnsweredByTheSeam(t *testing.T) {
	ext := installLoadSeamExtension(t)
	enqueued := countDDEnqueue(t)
	node := newLoadSeamProxy()

	request := &milvuspb.ReleaseCollectionRequest{DbName: "db", CollectionName: "coll"}
	status, err := node.ReleaseCollection(context.Background(), request)

	require.NoError(t, err)
	assert.Same(t, ext.releaseCollectionStatus, status)
	assert.Zero(t, *enqueued, "a release the extension answered must never reach the task queue")
	assert.Same(t, request, ext.sawReleaseCollection)
}

func TestReleaseCollectionReachesTheNativePathWithNoProviderInstalled(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	enqueued := countDDEnqueue(t)
	node := newLoadSeamProxy()

	status, err := node.ReleaseCollection(context.Background(), &milvuspb.ReleaseCollectionRequest{
		DbName:         "db",
		CollectionName: "coll",
	})

	require.NoError(t, err)
	assert.Equal(t, 1, *enqueued, "with no provider installed the release must reach the native task path")
	assert.Equal(t, errEnqueueRefused.Error(), status.GetReason())
}

func TestLoadPartitionsIsAnsweredByTheSeam(t *testing.T) {
	ext := installLoadSeamExtension(t)
	enqueued := countDDEnqueue(t)
	node := newLoadSeamProxy()

	request := &milvuspb.LoadPartitionsRequest{DbName: "db", CollectionName: "coll", PartitionNames: []string{"p1"}}
	status, err := node.LoadPartitions(context.Background(), request)

	require.NoError(t, err)
	assert.Same(t, ext.loadPartitionsStatus, status)
	assert.Zero(t, *enqueued, "a load the extension answered must never reach the task queue")
	assert.Same(t, request, ext.sawLoadPartitions)
}

func TestLoadPartitionsReachesTheNativePathWithNoProviderInstalled(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	enqueued := countDDEnqueue(t)
	node := newLoadSeamProxy()

	status, err := node.LoadPartitions(context.Background(), &milvuspb.LoadPartitionsRequest{
		DbName:         "db",
		CollectionName: "coll",
		PartitionNames: []string{"p1"},
	})

	require.NoError(t, err)
	assert.Equal(t, 1, *enqueued, "with no provider installed the load must reach the native task path")
	assert.Equal(t, errEnqueueRefused.Error(), status.GetReason())
}

func TestReleasePartitionsIsAnsweredByTheSeam(t *testing.T) {
	ext := installLoadSeamExtension(t)
	enqueued := countDDEnqueue(t)
	node := newLoadSeamProxy()

	request := &milvuspb.ReleasePartitionsRequest{DbName: "db", CollectionName: "coll", PartitionNames: []string{"p1"}}
	status, err := node.ReleasePartitions(context.Background(), request)

	require.NoError(t, err)
	assert.Same(t, ext.releasePartitionsStatus, status)
	assert.Zero(t, *enqueued, "a release the extension answered must never reach the task queue")
	assert.Same(t, request, ext.sawReleasePartitions)
}

func TestReleasePartitionsReachesTheNativePathWithNoProviderInstalled(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	enqueued := countDDEnqueue(t)
	node := newLoadSeamProxy()

	status, err := node.ReleasePartitions(context.Background(), &milvuspb.ReleasePartitionsRequest{
		DbName:         "db",
		CollectionName: "coll",
		PartitionNames: []string{"p1"},
	})

	require.NoError(t, err)
	assert.Equal(t, 1, *enqueued, "with no provider installed the release must reach the native task path")
	assert.Equal(t, errEnqueueRefused.Error(), status.GetReason())
}

// ---------------------------------------------------------------------------
// The refresh fall-through.
// ---------------------------------------------------------------------------

// TestLoadCollectionRefreshFallsThroughToNative and its partitions twin are the
// contract's sharp edge, driven through the production entry points.
//
// A refresh is not a load: querycoord answers it from a branch of its own that
// re-pulls the target of an already-loaded collection, and it is the only way a
// client can ask for that re-read. So an extension that answers ordinary loads
// returns nil for a refresh, and the seam has to honor the nil by carrying on
// into the native path even though an extension IS installed.
//
// Both halves are asserted against the same extension in the same test: the
// ordinary load stops at the seam and the refresh does not. A seam that
// short-circuits on any non-nil-extension - the obvious wrong implementation -
// fails the second half, and a seam that never short-circuits fails the first.
func TestLoadCollectionRefreshFallsThroughToNative(t *testing.T) {
	ext := installLoadSeamExtension(t)
	enqueued := countDDEnqueue(t)
	node := newLoadSeamProxy()

	ordinary, err := node.LoadCollection(context.Background(), &milvuspb.LoadCollectionRequest{
		DbName:         "db",
		CollectionName: "coll",
	})
	require.NoError(t, err)
	assert.Same(t, ext.loadCollectionStatus, ordinary, "an ordinary load is answered by the extension")
	require.Zero(t, *enqueued, "an ordinary load must not reach the task queue")

	refresh, err := node.LoadCollection(context.Background(), &milvuspb.LoadCollectionRequest{
		DbName:         "db",
		CollectionName: "coll",
		Refresh:        true,
	})
	require.NoError(t, err)
	assert.True(t, ext.sawLoadCollection.GetRefresh(),
		"the seam must consult the extension for a refresh too: falling through is the extension's decision to make, not the seam's")
	assert.Equal(t, 1, *enqueued,
		"a refresh must reach the native path, or the client is told its data was re-read when nothing re-read it")
	assert.Equal(t, errEnqueueRefused.Error(), refresh.GetReason(),
		"the refresh must be answered by the native path, not by the extension's success status")
	assert.NotSame(t, ext.loadCollectionStatus, refresh)
}

func TestLoadPartitionsRefreshFallsThroughToNative(t *testing.T) {
	ext := installLoadSeamExtension(t)
	enqueued := countDDEnqueue(t)
	node := newLoadSeamProxy()

	ordinary, err := node.LoadPartitions(context.Background(), &milvuspb.LoadPartitionsRequest{
		DbName:         "db",
		CollectionName: "coll",
		PartitionNames: []string{"p1"},
	})
	require.NoError(t, err)
	assert.Same(t, ext.loadPartitionsStatus, ordinary, "an ordinary load is answered by the extension")
	require.Zero(t, *enqueued, "an ordinary load must not reach the task queue")

	refresh, err := node.LoadPartitions(context.Background(), &milvuspb.LoadPartitionsRequest{
		DbName:         "db",
		CollectionName: "coll",
		PartitionNames: []string{"p1"},
		Refresh:        true,
	})
	require.NoError(t, err)
	assert.True(t, ext.sawLoadPartitions.GetRefresh(),
		"the seam must consult the extension for a refresh too")
	assert.Equal(t, 1, *enqueued,
		"a refresh must reach the native path, or the client is told its data was re-read when nothing re-read it")
	assert.Equal(t, errEnqueueRefused.Error(), refresh.GetReason())
	assert.NotSame(t, ext.loadPartitionsStatus, refresh)
}

// ---------------------------------------------------------------------------
// The two response entry points.
// ---------------------------------------------------------------------------

func TestGetLoadStateIsAnsweredByTheSeam(t *testing.T) {
	ext := installLoadSeamExtension(t)
	oldCache := globalMetaCache
	t.Cleanup(func() { globalMetaCache = oldCache })

	// No expectation is registered: an unexpected lookup fails the test on its
	// own, ahead of the explicit call-count assertion below.
	cache := NewMockCache(t)
	globalMetaCache = cache

	node := newLoadSeamProxy()
	request := &milvuspb.GetLoadStateRequest{DbName: "db", CollectionName: "coll"}
	resp, err := node.GetLoadState(context.Background(), request)

	require.NoError(t, err)
	assert.Same(t, ext.loadStateResponse, resp,
		"the handler must return the extension's response as it stands, status included")
	cache.AssertNumberOfCalls(t, "GetCollectionID", 0)
	assert.Same(t, request, ext.sawLoadState)
}

func TestGetLoadStateReachesTheNativePathWithNoProviderInstalled(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	oldCache := globalMetaCache
	t.Cleanup(func() { globalMetaCache = oldCache })

	cache := NewMockCache(t)
	cache.On("GetCollectionID", mock.Anything, "db", "coll").
		Return(UniqueID(0), merr.WrapErrCollectionNotFound("coll"))
	globalMetaCache = cache

	node := newLoadSeamProxy()
	resp, err := node.GetLoadState(context.Background(), &milvuspb.GetLoadStateRequest{
		DbName:         "db",
		CollectionName: "coll",
	})

	require.NoError(t, err)
	cache.AssertNumberOfCalls(t, "GetCollectionID", 1)
	assert.Equal(t, commonpb.LoadState_LoadStateNotExist, resp.GetState(),
		"with no provider installed the state must be the one milvus itself worked out")
}

func TestGetLoadingProgressIsAnsweredByTheSeam(t *testing.T) {
	ext := installLoadSeamExtension(t)
	oldCache := globalMetaCache
	t.Cleanup(func() { globalMetaCache = oldCache })

	cache := NewMockCache(t)
	globalMetaCache = cache

	node := newLoadSeamProxy()
	request := &milvuspb.GetLoadingProgressRequest{DbName: "db", CollectionName: "coll"}
	resp, err := node.GetLoadingProgress(context.Background(), request)

	require.NoError(t, err)
	assert.Same(t, ext.loadingProgressResponse, resp)
	cache.AssertNumberOfCalls(t, "GetCollectionID", 0)
	assert.Same(t, request, ext.sawLoadingProgress)
}

func TestGetLoadingProgressReachesTheNativePathWithNoProviderInstalled(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	oldCache := globalMetaCache
	t.Cleanup(func() { globalMetaCache = oldCache })

	notFound := merr.WrapErrCollectionNotFound("coll")
	cache := NewMockCache(t)
	cache.On("GetCollectionID", mock.Anything, "db", "coll").Return(UniqueID(0), notFound)
	globalMetaCache = cache

	node := newLoadSeamProxy()
	resp, err := node.GetLoadingProgress(context.Background(), &milvuspb.GetLoadingProgressRequest{
		DbName:         "db",
		CollectionName: "coll",
	})

	require.NoError(t, err)
	cache.AssertNumberOfCalls(t, "GetCollectionID", 1)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrCollectionNotFound,
		"with no provider installed the answer must be milvus's own")
	assert.Zero(t, resp.GetProgress())
}

// ---------------------------------------------------------------------------
// Ordering against the handler's own health check.
// ---------------------------------------------------------------------------

// TestLoadSemanticsSeamsAreNotConsultedByAnUnhealthyProxy pins where in the
// handler each seam sits: behind milvus's own health check. A proxy that cannot
// serve answers for itself, and consulting an extension first would let a form
// report a load as done on a proxy that is in no position to have done it.
func TestLoadSemanticsSeamsAreNotConsultedByAnUnhealthyProxy(t *testing.T) {
	ext := installLoadSeamExtension(t)

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	ctx := context.Background()

	loadCollection, err := node.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: "coll"})
	require.NoError(t, err)
	releaseCollection, err := node.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: "coll"})
	require.NoError(t, err)
	loadPartitions, err := node.LoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{CollectionName: "coll"})
	require.NoError(t, err)
	releasePartitions, err := node.ReleasePartitions(ctx, &milvuspb.ReleasePartitionsRequest{CollectionName: "coll"})
	require.NoError(t, err)
	loadState, err := node.GetLoadState(ctx, &milvuspb.GetLoadStateRequest{CollectionName: "coll"})
	require.NoError(t, err)
	loadingProgress, err := node.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{CollectionName: "coll"})
	require.NoError(t, err)

	assert.Zero(t, ext.consulted,
		"an unhealthy proxy must answer for itself: no seam in the group may be consulted before the health check")

	for name, err := range map[string]error{
		"LoadCollection":     merr.Error(loadCollection),
		"ReleaseCollection":  merr.Error(releaseCollection),
		"LoadPartitions":     merr.Error(loadPartitions),
		"ReleasePartitions":  merr.Error(releasePartitions),
		"GetLoadState":       merr.Error(loadState.GetStatus()),
		"GetLoadingProgress": merr.Error(loadingProgress.GetStatus()),
	} {
		assert.ErrorIs(t, err, merr.ErrServiceNotReady,
			"%s on an unhealthy proxy must answer with the health error, not with the extension's success", name)
	}
}
