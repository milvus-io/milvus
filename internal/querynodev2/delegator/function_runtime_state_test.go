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

package delegator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestFunctionRuntimeStateEnsureFuncStateSkipsStaleSchema(t *testing.T) {
	paramtable.Init()
	state, err := buildFunctionRuntimeState(newFunctionRuntimeTestSchemaWithVersion(2, newMinHashFunctionSchema()))
	require.NoError(t, err)
	require.True(t, state.hasFunctionType(104, schemapb.FunctionType_MinHash))

	require.NoError(t, state.ensureFuncState(newFunctionRuntimeTestSchemaWithVersion(1)))
	assert.True(t, state.hasFunctionType(104, schemapb.FunctionType_MinHash))
}

func TestFunctionRuntimeStateSwapNeverRollsBack(t *testing.T) {
	paramtable.Init()
	state, err := buildFunctionRuntimeState(newFunctionRuntimeTestSchemaWithVersion(2, newMinHashFunctionSchema()))
	require.NoError(t, err)

	older, err := buildFunctionRuntimeState(newFunctionRuntimeTestSchemaWithVersion(1))
	require.NoError(t, err)
	state.swap(older).Close()
	assert.True(t, state.hasFunctionType(104, schemapb.FunctionType_MinHash))

	newer, err := buildFunctionRuntimeState(newFunctionRuntimeTestSchemaWithVersion(3, newBM25FunctionSchema()))
	require.NoError(t, err)
	state.swap(newer).Close()
	assert.False(t, state.hasFunctionType(104, schemapb.FunctionType_MinHash))
	assert.True(t, state.hasFunctionType(102, schemapb.FunctionType_BM25))
}

// Reproduces the end state of the add-function-field race: segment load
// (collectionManager.PutOrRef) advanced the collection schema snapshot before
// the delegator's UpdateSchema event ran, so the guarded UpdateSchema became a
// no-op and the function runtime state was never rebuilt. A search against the
// added MinHash output field must still trigger text-to-vector conversion
// instead of silently forwarding the VARCHAR placeholder to segcore.
func TestPrepareSearchFunctionRefreshesStaleStateFromSnapshot(t *testing.T) {
	paramtable.Init()
	staleState, err := buildFunctionRuntimeState(newFunctionRuntimeTestSchemaWithVersion(0))
	require.NoError(t, err)

	advancedSchema := newFunctionRuntimeTestSchemaWithVersion(1, newMinHashFunctionSchema())
	sd := &shardDelegator{
		collectionID:  1000,
		vchannelName:  "test-channel",
		collection:    segments.NewCollectionWithoutSegcoreForTest(1000, advancedSchema),
		functionState: staleState,
	}

	defer sd.releaseFunctionRunners()

	_, _, err = sd.prepareSearchFunction(context.Background(), &internalpb.SearchRequest{FieldId: 104})
	// The stale gate is refreshed and the MinHash branch is entered: the empty
	// placeholder is rejected by parseMinHash rather than silently passed through.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MinHash")
	assert.True(t, sd.functionState.hasFunctionType(104, schemapb.FunctionType_MinHash))

	// The lazy refresh must also register the function runner the skipped
	// UpdateSchema event would have registered, so a real search can
	// materialize the text-to-vector conversion.
	ok, err := function.RunWithRunner(context.Background(), 1000, 1, 104, func(functionType schemapb.FunctionType, _ function.FunctionRunner) error {
		assert.Equal(t, schemapb.FunctionType_MinHash, functionType)
		return nil
	})
	require.NoError(t, err)
	assert.True(t, ok, "MinHash function runner not registered by the lazy refresh")
}

// A schema snapshot that fails the function-state rebuild must fail the search
// with the build error instead of silently skipping conversion.
func TestPrepareSearchFunctionPropagatesRebuildError(t *testing.T) {
	paramtable.Init()
	staleState, err := buildFunctionRuntimeState(newFunctionRuntimeTestSchemaWithVersion(0))
	require.NoError(t, err)

	malformed := newFunctionRuntimeTestSchemaWithVersion(1, &schemapb.FunctionSchema{
		Type:           schemapb.FunctionType_MinHash,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{999},
	})
	sd := &shardDelegator{
		collectionID:  1001,
		vchannelName:  "test-channel-err",
		collection:    segments.NewCollectionWithoutSegcoreForTest(1001, malformed),
		functionState: staleState,
	}
	defer sd.releaseFunctionRunners()

	_, _, err = sd.prepareSearchFunction(context.Background(), &internalpb.SearchRequest{FieldId: 999})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "output field")
}
