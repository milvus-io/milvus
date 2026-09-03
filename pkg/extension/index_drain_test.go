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

package extension

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// stubIndexDrainer answers every question the same way, which is enough for
// the capability-table tests: what a real drainer decides is exercised where
// the seams call it.
type stubIndexDrainer struct{ answer bool }

func (s stubIndexDrainer) AllowVectorIndexDropWhileLoaded(context.Context, int64, string) bool {
	return s.answer
}

func (s stubIndexDrainer) BeginDropIndex(context.Context, *indexpb.DropIndexRequest) bool {
	return s.answer
}

func (stubIndexDrainer) AfterDropIndex(context.Context, *indexpb.DropIndexRequest) {}

func (stubIndexDrainer) AbortDropIndex(context.Context, *indexpb.DropIndexRequest) {}

func (stubIndexDrainer) AfterCreateIndex(context.Context, *indexpb.CreateIndexRequest) {}

func (s stubIndexDrainer) CollectionDraining(context.Context, int64) bool { return s.answer }

func TestIndexDrainerAbsentWithoutProvider(t *testing.T) {
	ResetForTest()
	assert.Nil(t, Caps().IndexDrain,
		"with no provider installed the index paths must have nothing to consult")
}

func TestIndexDrainerIsInstalledAndRequirable(t *testing.T) {
	ResetForTest()
	defer ResetForTest()

	drainer := stubIndexDrainer{answer: true}
	err := SetProvider(fakeProvider{
		name:     "testprovider",
		requires: []CapabilityID{CapIndexDrain},
		caps:     Capabilities{IndexDrain: drainer},
	})
	assert.NoError(t, err)
	assert.Equal(t, drainer, Caps().IndexDrain)
}

func TestSetProviderRejectsMissingIndexDrainer(t *testing.T) {
	ResetForTest()
	defer ResetForTest()

	err := SetProvider(fakeProvider{
		name:     "testprovider",
		requires: []CapabilityID{CapIndexDrain},
		caps:     Capabilities{},
	})
	assert.ErrorContains(t, err, string(CapIndexDrain),
		"a form that declared it drops vector indexes gracefully must not start without a drainer")
	assert.Nil(t, Caps().IndexDrain, "a failed install must leave no trace")
}

// The two capabilities are separate table entries so that a form can install
// one without the other; installing one must not make the other appear.
func TestResourceGroupAndIndexDrainAreIndependent(t *testing.T) {
	ResetForTest()
	defer ResetForTest()

	assert.NoError(t, SetProvider(fakeProvider{
		name: "testprovider",
		caps: Capabilities{IndexDrain: stubIndexDrainer{}},
	}))
	assert.NotNil(t, Caps().IndexDrain)
	assert.Nil(t, Caps().ResourceGroups)
}

// The Noop drainer keeps milvus's refusal: a form that embeds it and overrides
// nothing must not have suppressed the one protection a loaded collection has.
func TestNoopIndexDrainerKeepsTheRefusal(t *testing.T) {
	type embedder struct{ NoopIndexDrainer }
	var d IndexDrainer = embedder{}
	ctx := context.Background()

	assert.False(t, d.AllowVectorIndexDropWhileLoaded(ctx, 1, "idx"),
		"the inert answer must leave the native refusal in place")
	assert.False(t, d.BeginDropIndex(ctx, &indexpb.DropIndexRequest{}),
		"the inert answer must not ask for AfterDropIndex")
	assert.False(t, d.CollectionDraining(ctx, 1))
	assert.NotPanics(t, func() {
		d.AfterDropIndex(ctx, &indexpb.DropIndexRequest{})
		d.AbortDropIndex(ctx, &indexpb.DropIndexRequest{})
		d.AfterCreateIndex(ctx, &indexpb.CreateIndexRequest{})
	})
}
