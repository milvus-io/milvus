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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// prepareSearchFunction serves from the ready snapshot's function state (not a
// lazily refreshed live view): given a snapshot whose schema carries the added
// MinHash output field, a search against it enters the MinHash branch and the
// empty placeholder is rejected rather than silently passed through to segcore.
func TestPrepareSearchFunctionUsesSnapshotFunctionState(t *testing.T) {
	paramtable.Init()
	advancedSchema := newFunctionRuntimeTestSchemaWithVersion(1, newMinHashFunctionSchema())
	sd := &shardDelegator{
		collectionID: 1000,
		vchannelName: "test-channel",
		collection:   segments.NewCollectionWithoutSegcoreForTest(1000, advancedSchema),
	}
	defer sd.releaseFunctionRunners()
	// The publish path registers the runners before publishing the snapshot.
	sd.updateFunctionRunners(advancedSchema)

	fs, err := buildFunctionRuntimeState(advancedSchema)
	require.NoError(t, err)
	snap := newReadySnapshot(1, 0, advancedSchema, fs)

	_, _, err = sd.prepareSearchFunction(context.Background(), snap, &internalpb.SearchRequest{FieldId: 104})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MinHash")

	// The runner the publish path registered is materializable for a real search.
	ok, err := function.RunWithRunner(context.Background(), 1000, 1, 104, func(functionType schemapb.FunctionType, _ function.FunctionRunner) error {
		assert.Equal(t, schemapb.FunctionType_MinHash, functionType)
		return nil
	})
	require.NoError(t, err)
	assert.True(t, ok, "MinHash function runner not registered")
}

// A version-gated search whose version the snapshot already covers, targeting
// a field ABSENT from the snapshot (the field was dropped — BM25/MinHash
// drops always take their output field with them), must be rejected
// explicitly: forwarding it would fail opaquely downstream (or ship a raw
// VarChar placeholder to segcore for text searches) and blacklist a healthy
// delegator. Version-0 legacy requests skip the guard — for them the same
// state can mean "field just added, snapshot behind", which must stay
// serveable-after-retry.
func TestPrepareSearchFunctionDroppedFieldGuardIsVersionScoped(t *testing.T) {
	paramtable.Init()
	droppedSchema := newFunctionRuntimeTestSchemaWithVersion(3)
	sd := &shardDelegator{
		collectionID: 1000,
		vchannelName: "test-channel",
		collection:   segments.NewCollectionWithoutSegcoreForTest(1000, droppedSchema),
	}
	fs, err := buildFunctionRuntimeState(droppedSchema)
	require.NoError(t, err)
	snap := newReadySnapshot(3, 0, droppedSchema, fs)

	// Version-gated request (v2 <= snapshot v3) on a field absent from the
	// snapshot: deterministic drop, rejected — for any metric (covers dropped
	// BM25 sparse fields, dropped MinHash binary fields, plain vector fields).
	for _, metricType := range []string{"BM25", "MHJACCARD", "L2"} {
		_, skip, err := sd.prepareSearchFunction(context.Background(), snap, &internalpb.SearchRequest{
			FieldId:                 777, // not in the schema
			MetricType:              metricType,
			CollectionSchemaVersion: 2,
		})
		require.Error(t, err, metricType)
		assert.False(t, skip)
		assert.Contains(t, err.Error(), "has since been dropped", metricType)
	}

	// Legacy request (version 0): guard skipped, no error from this layer.
	_, skip, err := sd.prepareSearchFunction(context.Background(), snap, &internalpb.SearchRequest{
		FieldId:    777,
		MetricType: "BM25",
	})
	require.NoError(t, err)
	assert.False(t, skip)

	// A live field is unaffected by the guard.
	_, _, err = sd.prepareSearchFunction(context.Background(), snap, &internalpb.SearchRequest{
		FieldId:                 102,
		MetricType:              "L2",
		CollectionSchemaVersion: 2,
	})
	require.NoError(t, err)
}

// A search against a MinHash function output field must use the MHJACCARD
// metric (or leave it empty): the snapshot's function state enforces this
// before any placeholder parsing, mirroring the BM25 metric enforcement.
func TestPrepareSearchFunctionMinHashMetricEnforced(t *testing.T) {
	paramtable.Init()
	schema := newFunctionRuntimeTestSchemaWithVersion(1, newMinHashFunctionSchema())
	sd := &shardDelegator{
		collectionID: 1002,
		vchannelName: "test-channel",
		collection:   segments.NewCollectionWithoutSegcoreForTest(1002, schema),
	}
	fs, err := buildFunctionRuntimeState(schema)
	require.NoError(t, err)
	snap := newReadySnapshot(1, 0, schema, fs)

	_, _, err = sd.prepareSearchFunction(context.Background(), snap, &internalpb.SearchRequest{
		FieldId:    104,
		MetricType: "L2",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MHJACCARD")
}

// When the snapshot maps a field to a MinHash function but the runner registry
// no longer has it (the applied>published transition window: the function was
// dropped in a newer applied version and updateFunctionRunners GC'd it), the
// search must fail RETRIABLE — the proxy retries until the newer version
// publishes — never as a permanent internal error.
func TestPrepareSearchFunctionRunnerMissingIsRetriable(t *testing.T) {
	paramtable.Init()
	schema := newFunctionRuntimeTestSchemaWithVersion(2, newMinHashFunctionSchema())
	// Isolated collectionID with NO runners registered — models the GC'd state.
	sd := &shardDelegator{
		collectionID: 1003,
		vchannelName: "test-channel",
		collection:   segments.NewCollectionWithoutSegcoreForTest(1003, schema),
	}
	fs, err := buildFunctionRuntimeState(schema)
	require.NoError(t, err)
	snap := newReadySnapshot(2, 0, schema, fs)

	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{{
			Type:   commonpb.PlaceholderType_VarChar,
			Values: [][]byte{[]byte("hello world")},
		}},
	})
	require.NoError(t, err)

	_, _, err = sd.prepareSearchFunction(context.Background(), snap, &internalpb.SearchRequest{
		FieldId:          104,
		MetricType:       "MHJACCARD",
		PlaceholderGroup: placeholder,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrCollectionSchemaVersionNotReady),
		"runner missing during the schema transition must be retriable, got: %v", err)
}

// A malformed schema (function output field absent from the schema) must fail
// the snapshot build at publish time, so a broken version is never published to
// the read path.
func TestBuildFunctionRuntimeStateRejectsMalformedSchema(t *testing.T) {
	paramtable.Init()
	malformed := newFunctionRuntimeTestSchemaWithVersion(1, &schemapb.FunctionSchema{
		Type:           schemapb.FunctionType_MinHash,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{999},
	})

	_, err := buildFunctionRuntimeState(malformed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "output field")
}
