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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// countingComponent panics on a second Stop, like a component that closes a
// channel in Stop.
type countingComponent struct {
	stops  atomic.Int32
	stopCh chan struct{}
	err    error
}

func newCountingComponent(err error) *countingComponent {
	return &countingComponent{stopCh: make(chan struct{}), err: err}
}

func (c *countingComponent) GetName() string { return "counting" }
func (c *countingComponent) Health(context.Context) commonpb.StateCode {
	return commonpb.StateCode_Healthy
}
func (c *countingComponent) Prepare() error { return nil }
func (c *countingComponent) Run() error     { return nil }

func (c *countingComponent) Stop() error {
	c.stops.Add(1)
	close(c.stopCh)
	return c.err
}

func TestStopOnceComponent(t *testing.T) {
	stopErr := errors.New("stop failed")
	inner := newCountingComponent(stopErr)
	c := &stopOnceComponent{component: inner}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.ErrorIs(t, c.Stop(), stopErr)
		}()
	}
	wg.Wait()
	assert.ErrorIs(t, c.Stop(), stopErr)
	assert.EqualValues(t, 1, inner.stops.Load())
}

// The /management/stop handler and the shutdown sequence both stop the
// component taken from the map returned by waitForAllComponentsReady.
func TestWaitForAllComponentsReadyStopsOnce(t *testing.T) {
	inner := newCountingComponent(nil)
	mr := &MilvusRoles{closed: make(chan struct{})}
	futures := map[string]*conc.Future[component]{
		typeutil.MixCoordRole: conc.Go(func() (component, error) { return inner, nil }),
	}

	componentMap, err := mr.waitForAllComponentsReady(func() {}, futures)
	require.NoError(t, err)

	mixCoord := componentMap[typeutil.MixCoordRole]
	require.NoError(t, mixCoord.Stop()) // /management/stop?role=mixcoord
	require.NoError(t, mixCoord.Stop()) // shutdown after SIGTERM
	assert.EqualValues(t, 1, inner.stops.Load())
}
