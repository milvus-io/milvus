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

package roles

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type stubStartupComponent struct {
	name      string
	runFn     func() error
	stopCount atomic.Int32
}

func (c *stubStartupComponent) GetName() string { return c.name }

func (c *stubStartupComponent) Health(context.Context) commonpb.StateCode {
	return commonpb.StateCode_Healthy
}

func (c *stubStartupComponent) Prepare() error { return nil }

func (c *stubStartupComponent) Run() error {
	if c.runFn != nil {
		return c.runFn()
	}
	return nil
}

func (c *stubStartupComponent) Stop() error {
	c.stopCount.Add(1)
	return nil
}

func TestWaitForAllComponentsReadySiblingFailureCancelsAndCleansUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mr := &MilvusRoles{closed: make(chan struct{})}
	enteredReg := make(chan struct{})
	regDone := make(chan struct{})
	syntheticErr := errors.New("synthetic component initialization failure")

	readyComp := newCountingComponent(nil)
	futures := map[string]*conc.Future[component]{
		typeutil.QueryNodeRole: conc.Go(func() (component, error) {
			return readyComp, nil
		}),
		typeutil.StreamingNodeRole: conc.Go(func() (component, error) {
			defer close(regDone)
			close(enteredReg)
			<-ctx.Done()
			return nil, ctx.Err()
		}),
		typeutil.DataNodeRole: conc.Go(func() (component, error) {
			<-enteredReg
			return nil, syntheticErr
		}),
	}

	componentMap, err := mr.waitForAllComponentsReady(cancel, futures)
	require.Nil(t, componentMap)
	require.ErrorIs(t, err, syntheticErr)
	assert.False(t, errors.Is(err, context.Canceled))

	// All futures must have finished before waitForAllComponentsReady returns.
	select {
	case <-regDone:
	default:
		t.Fatal("expected canceled component future to finish before waitForAllComponentsReady returned")
	}

	assert.EqualValues(t, 1, readyComp.stops.Load())
}

func TestWaitForAllComponentsReadySignalCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mr := &MilvusRoles{closed: make(chan struct{})}
	entered := make(chan struct{})
	compDone := make(chan struct{})
	readyComp := newCountingComponent(nil)

	futures := map[string]*conc.Future[component]{
		typeutil.QueryNodeRole: conc.Go(func() (component, error) {
			return readyComp, nil
		}),
		typeutil.StreamingNodeRole: conc.Go(func() (component, error) {
			defer close(compDone)
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}),
	}

	<-entered
	close(mr.closed)

	componentMap, err := mr.waitForAllComponentsReady(cancel, futures)
	require.Nil(t, componentMap)
	require.ErrorIs(t, err, context.Canceled)

	select {
	case <-compDone:
	default:
		t.Fatal("expected component future to finish before waitForAllComponentsReady returned")
	}
	assert.EqualValues(t, 1, readyComp.stops.Load())
}

func TestRunComponentStopsOnRunFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := errors.New("run failed")
	comp := &stubStartupComponent{
		name:  "stub-run-fail",
		runFn: func() error { return runErr },
	}

	future := runComponent(ctx, false, func(context.Context, dependency.Factory) (*stubStartupComponent, error) {
		return comp, nil
	}, func(*prometheus.Registry) {})

	got, err := future.Await()
	require.Nil(t, got)
	require.ErrorIs(t, err, runErr)
	assert.EqualValues(t, 1, comp.stopCount.Load())
}

func TestWaitForAllComponentsReadyErrorThenSignalPreservesError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mr := &MilvusRoles{closed: make(chan struct{})}
	syntheticErr := errors.New("synthetic component initialization failure")
	enteredSibling := make(chan struct{})

	futures := map[string]*conc.Future[component]{
		typeutil.DataNodeRole: conc.Go(func() (component, error) {
			return nil, syntheticErr
		}),
		typeutil.QueryNodeRole: conc.Go(func() (component, error) {
			close(enteredSibling)
			<-ctx.Done()
			close(mr.closed)
			return nil, ctx.Err()
		}),
	}

	<-enteredSibling
	componentMap, err := mr.waitForAllComponentsReady(cancel, futures)
	require.Nil(t, componentMap)
	require.ErrorIs(t, err, syntheticErr)
	assert.False(t, mr.startupCanceledBySignal.Load())
}
