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

package function

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestEmbeddingFunctionSignatureIgnoresSchemaVersionAndUnrelatedSchema(t *testing.T) {
	base := newBM25SignatureTestSchema()
	baseSignature, err := EmbeddingFunctionSignature(base)
	require.NoError(t, err)

	schemaVersionChanged := cloneCollectionSchema(base)
	schemaVersionChanged.Version = base.GetVersion() + 1
	signature, err := EmbeddingFunctionSignature(schemaVersionChanged)
	require.NoError(t, err)
	require.Equal(t, baseSignature, signature)

	unrelatedFieldAdded := cloneCollectionSchema(base)
	unrelatedFieldAdded.Fields = append(unrelatedFieldAdded.Fields, &schemapb.FieldSchema{
		FieldID:  200,
		Name:     "extra",
		DataType: schemapb.DataType_Int64,
	})
	signature, err = EmbeddingFunctionSignature(unrelatedFieldAdded)
	require.NoError(t, err)
	require.Equal(t, baseSignature, signature)

	nonEmbeddingFunctionChanged := cloneCollectionSchema(base)
	nonEmbeddingFunctionChanged.Functions = append(nonEmbeddingFunctionChanged.Functions, &schemapb.FunctionSchema{
		Id:             200,
		Name:           "text_embedding",
		Type:           schemapb.FunctionType_TextEmbedding,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{103},
		Params: []*commonpb.KeyValuePair{
			{Key: "provider", Value: "mock"},
			{Key: "credential", Value: "changed"},
		},
	})
	signature, err = EmbeddingFunctionSignature(nonEmbeddingFunctionChanged)
	require.NoError(t, err)
	require.Equal(t, baseSignature, signature)
}

func TestEmbeddingFunctionSignatureChangesWhenRunnerInputsChange(t *testing.T) {
	base := newBM25SignatureTestSchema()
	baseSignature, err := EmbeddingFunctionSignature(base)
	require.NoError(t, err)

	functionParamChanged := cloneCollectionSchema(base)
	functionParamChanged.Functions[0].Params = []*commonpb.KeyValuePair{
		{Key: "rebuild", Value: "true"},
	}
	signature, err := EmbeddingFunctionSignature(functionParamChanged)
	require.NoError(t, err)
	require.NotEqual(t, baseSignature, signature)

	analyzerParamChanged := cloneCollectionSchema(base)
	analyzerParamChanged.Fields[1].TypeParams = []*commonpb.KeyValuePair{
		{Key: analyzerParams, Value: `{"tokenizer": "standard"}`},
	}
	signature, err = EmbeddingFunctionSignature(analyzerParamChanged)
	require.NoError(t, err)
	require.NotEqual(t, baseSignature, signature)
}

func TestEmbeddingOutputFieldIDsReturnsAllFunctionOutputs(t *testing.T) {
	schema := newBM25SignatureTestSchema()
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:          104,
		Name:             "sparse_extra",
		DataType:         schemapb.DataType_SparseFloatVector,
		IsFunctionOutput: true,
	})
	schema.Functions[0].OutputFieldIds = []int64{102, 104}

	outputFieldIDs, err := EmbeddingOutputFieldIDs(schema)
	require.NoError(t, err)
	require.Equal(t, []int64{102, 104}, outputFieldIDs)
}

func TestHasEmbeddingFunctionsOnlyMatchesRunnerBackedFunctions(t *testing.T) {
	schema := newBM25SignatureTestSchema()
	require.True(t, HasEmbeddingFunctions(schema))

	schema.Functions = nil
	require.False(t, HasEmbeddingFunctions(schema))

	schema.Functions = []*schemapb.FunctionSchema{
		{
			Id:             200,
			Name:           "text_embedding",
			Type:           schemapb.FunctionType_TextEmbedding,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{103},
		},
	}
	require.False(t, HasEmbeddingFunctions(schema))
}

func TestFunctionRunnerManagerAllocRequiresSchema(t *testing.T) {
	manager, _ := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	err := manager.Alloc(1, "v1", nil)
	require.ErrorContains(t, err, "collection schema is nil")
	requireFunctionRunnerEntryRemoved(t, manager, 1)
}

func TestFunctionRunnerManagerAllocSkipsSchemaWithoutEmbeddingFunctions(t *testing.T) {
	manager := newFunctionRunnerManager()
	t.Cleanup(manager.Close)

	schema := cloneCollectionSchema(newBM25SignatureTestSchema())
	schema.Functions = nil

	require.NoError(t, manager.Alloc(1, "v1", schema))
	requireFunctionRunnerEntryRemoved(t, manager, 1)
}

func TestFunctionRunnerManagerAllocSkipsSchemaWithoutRunnerBackedFunctions(t *testing.T) {
	manager := newFunctionRunnerManager()
	t.Cleanup(manager.Close)

	schema := cloneCollectionSchema(newBM25SignatureTestSchema())
	schema.Functions = []*schemapb.FunctionSchema{
		{
			Id:             200,
			Name:           "text_embedding",
			Type:           schemapb.FunctionType_TextEmbedding,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{103},
		},
	}

	require.NoError(t, manager.Alloc(1, "v1", schema))
	requireFunctionRunnerEntryRemoved(t, manager, 1)
}

func TestFunctionRunnerManagerUpdateCreatesEntryWhenEmbeddingFunctionAppears(t *testing.T) {
	manager, factory := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	schemaWithoutFunctions := cloneCollectionSchema(newBM25SignatureTestSchema())
	schemaWithoutFunctions.Functions = nil
	require.NoError(t, manager.Alloc(1, "v1", schemaWithoutFunctions))
	requireFunctionRunnerEntryRemoved(t, manager, 1)

	schemaWithFunction := newBM25SignatureTestSchema()
	schemaWithFunction.Version = 2
	require.NoError(t, manager.Update(1, "v1", schemaWithFunction))

	keyVersions, versionRunners, runnerCount := functionRunnerEntrySnapshot(t, manager, 1)
	require.Equal(t, map[string]int32{"v1": 2}, keyVersions)
	require.Len(t, versionRunners, 1)
	require.Equal(t, 1, runnerCount)
	requireRunnerByOutput(t, manager, 1, schemaWithFunction.GetVersion(), 102)
	require.Equal(t, int32(1), factory.buildCount.Load())
}

func TestFunctionRunnerManagerUpdateReleasesKeyWhenEmbeddingFunctionsDisappear(t *testing.T) {
	manager, _ := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	schema := newBM25SignatureTestSchema()
	require.NoError(t, manager.Alloc(1, "v1", schema))
	runner := requireRunnerByOutput(t, manager, 1, schema.GetVersion(), 102)

	schemaWithoutFunctions := cloneCollectionSchema(schema)
	schemaWithoutFunctions.Version = 2
	schemaWithoutFunctions.Functions = nil
	require.NoError(t, manager.Update(1, "v1", schemaWithoutFunctions))

	requireFunctionRunnerEntryRemoved(t, manager, 1)
	require.True(t, runner.isClosed())
}

func TestFunctionRunnerManagerAllocTracksKeysBySchemaVersion(t *testing.T) {
	manager, factory := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	schema := newBM25SignatureTestSchema()
	require.NoError(t, manager.Alloc(1, "v1", schema))
	require.NoError(t, manager.Alloc(1, "v2", schema))

	keyVersions, versionRunners, runnerCount := functionRunnerEntrySnapshot(t, manager, 1)
	require.Equal(t, map[string]int32{"v1": 1, "v2": 1}, keyVersions)
	require.Len(t, versionRunners, 1)
	require.Equal(t, 1, runnerCount)

	baseRunner := requireRunnerByOutput(t, manager, 1, schema.GetVersion(), 102)
	require.Equal(t, int32(1), factory.buildCount.Load())

	manager.Release(1, "v1")
	require.False(t, baseRunner.isClosed())
	keyVersions, _, _ = functionRunnerEntrySnapshot(t, manager, 1)
	require.Len(t, keyVersions, 1)

	manager.Release(1, "v2")
	requireFunctionRunnerEntryRemoved(t, manager, 1)
	require.True(t, baseRunner.isClosed())
}

func TestFunctionRunnerManagerReleaseWaitsForAllKeys(t *testing.T) {
	manager, _ := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	schema := newBM25SignatureTestSchema()
	require.NoError(t, manager.Alloc(1, "WAL-v1", schema))
	require.NoError(t, manager.Alloc(1, "DELEGATOR-v1", schema))

	walVersion := functionRunnerKeyVersionSnapshot(t, manager, 1, "WAL-v1")
	require.Equal(t, int32(1), walVersion)
	delegatorVersion := functionRunnerKeyVersionSnapshot(t, manager, 1, "DELEGATOR-v1")
	require.Equal(t, int32(1), delegatorVersion)

	baseRunner := requireRunnerByOutput(t, manager, 1, schema.GetVersion(), 102)
	manager.Release(1, "WAL-v1")
	require.False(t, baseRunner.isClosed())

	ok, err := manager.RunWithAnalyzer(context.Background(), 1, schema.GetVersion(), 101, func(Analyzer) error {
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)

	manager.Release(1, "DELEGATOR-v1")
	requireFunctionRunnerEntryRemoved(t, manager, 1)
	require.True(t, baseRunner.isClosed())
}

func TestFunctionRunnerManagerKeepsOldVersionUntilAllKeysAdvance(t *testing.T) {
	manager, _ := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	base := newBM25SignatureTestSchema()
	changedOutput := newSchemaWithChangedOutput(base)
	require.NoError(t, manager.Alloc(1, "WAL-v1", base))
	require.NoError(t, manager.Alloc(1, "DELEGATOR-v1", base))
	baseRunner := requireRunnerByOutput(t, manager, 1, base.GetVersion(), 102)

	require.NoError(t, manager.Update(1, "WAL-v1", changedOutput))
	changedRunner := requireRunnerByOutput(t, manager, 1, changedOutput.GetVersion(), 104)
	require.False(t, baseRunner.isClosed())
	require.False(t, changedRunner.isClosed())

	_, versionRunners, runnerCount := functionRunnerEntrySnapshot(t, manager, 1)
	require.Contains(t, versionRunners, int32(1))
	require.Contains(t, versionRunners, int32(2))
	require.Equal(t, 2, runnerCount)

	require.NoError(t, manager.Update(1, "DELEGATOR-v1", changedOutput))
	require.True(t, baseRunner.isClosed())
	require.False(t, changedRunner.isClosed())

	_, versionRunners, runnerCount = functionRunnerEntrySnapshot(t, manager, 1)
	require.NotContains(t, versionRunners, int32(1))
	require.Contains(t, versionRunners, int32(2))
	require.Equal(t, 1, runnerCount)

	manager.Release(1, "WAL-v1")
	require.False(t, changedRunner.isClosed())
	manager.Release(1, "DELEGATOR-v1")
	requireFunctionRunnerEntryRemoved(t, manager, 1)
	require.True(t, changedRunner.isClosed())
}

func TestFunctionRunnerManagerUpdateReusesSameSignatureAcrossSchemaVersions(t *testing.T) {
	manager, factory := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	base := newBM25SignatureTestSchema()
	require.NoError(t, manager.Alloc(1, "v1", base))
	baseRunner := requireRunnerByOutput(t, manager, 1, base.GetVersion(), 102)

	schemaVersionChanged := cloneCollectionSchema(base)
	schemaVersionChanged.Version = 2
	require.NoError(t, manager.Update(1, "v1", schemaVersionChanged))

	ok, err := manager.RunWithRunner(context.Background(), 1, schemaVersionChanged.GetVersion(), 102, func(functionType schemapb.FunctionType, runner FunctionRunner) error {
		require.Equal(t, schemapb.FunctionType_BM25, functionType)
		require.True(t, baseRunner == runner)
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, baseRunner.isClosed())
	require.Equal(t, int32(1), factory.buildCount.Load())

	_, versionRunners, runnerCount := functionRunnerEntrySnapshot(t, manager, 1)
	require.Contains(t, versionRunners, int32(2))
	require.NotContains(t, versionRunners, int32(1))
	require.Equal(t, 1, runnerCount)
}

func TestFunctionRunnerManagerUpdateKeepsOldVersionUntilAllKeysAdvance(t *testing.T) {
	manager, _ := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	base := newBM25SignatureTestSchema()
	changedOutput := newSchemaWithChangedOutput(base)

	require.NoError(t, manager.Alloc(1, "v1", base))
	require.NoError(t, manager.Alloc(1, "v2", base))
	baseRunner := requireRunnerByOutput(t, manager, 1, base.GetVersion(), 102)

	require.NoError(t, manager.Update(1, "v1", changedOutput))
	changedRunner := requireRunnerByOutput(t, manager, 1, changedOutput.GetVersion(), 104)
	require.False(t, baseRunner.isClosed())
	require.False(t, changedRunner.isClosed())

	_, versionRunners, runnerCount := functionRunnerEntrySnapshot(t, manager, 1)
	require.Contains(t, versionRunners, int32(1))
	require.Contains(t, versionRunners, int32(2))
	require.Equal(t, 2, runnerCount)

	oldBody := newBM25InsertRequest("old message")
	changed, ok, err := manager.TryMaterialize(1, 1, oldBody)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, ok)
	require.True(t, HasFieldData(oldBody.GetFieldsData(), 102))
	require.False(t, HasFieldData(oldBody.GetFieldsData(), 104))

	newBody := newBM25InsertRequest("new message")
	changed, ok, err = manager.TryMaterialize(1, 2, newBody)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, ok)
	require.False(t, HasFieldData(newBody.GetFieldsData(), 102))
	require.True(t, HasFieldData(newBody.GetFieldsData(), 104))

	require.NoError(t, manager.Update(1, "v2", changedOutput))
	require.True(t, baseRunner.isClosed())
	require.False(t, changedRunner.isClosed())

	_, versionRunners, runnerCount = functionRunnerEntrySnapshot(t, manager, 1)
	require.NotContains(t, versionRunners, int32(1))
	require.Contains(t, versionRunners, int32(2))
	require.Equal(t, 1, runnerCount)

	changed, ok, err = manager.TryMaterialize(1, 1, newBM25InsertRequest("old message"))
	require.NoError(t, err)
	require.False(t, changed)
	require.False(t, ok)
}

func TestFunctionRunnerManagerUpdateBuildsOnlyAddedFunction(t *testing.T) {
	manager, factory := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	base := newBM25SignatureTestSchema()
	require.NoError(t, manager.Alloc(1, "v1", base))
	baseRunner := requireRunnerByOutput(t, manager, 1, base.GetVersion(), 102)

	addedFunction := newSchemaWithAddedFunction(base)
	require.NoError(t, manager.Update(1, "v1", addedFunction))
	addedRunner := requireRunnerByOutput(t, manager, 1, addedFunction.GetVersion(), 104)

	require.Equal(t, int32(2), factory.buildCount.Load())
	require.False(t, baseRunner.isClosed())
	require.False(t, addedRunner.isClosed())

	ok, err := manager.RunWithRunner(context.Background(), 1, addedFunction.GetVersion(), 102, func(functionType schemapb.FunctionType, runner FunctionRunner) error {
		require.Equal(t, schemapb.FunctionType_BM25, functionType)
		require.True(t, baseRunner == runner)
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = manager.RunWithRunner(context.Background(), 1, addedFunction.GetVersion(), 104, func(functionType schemapb.FunctionType, runner FunctionRunner) error {
		require.Equal(t, schemapb.FunctionType_BM25, functionType)
		require.True(t, addedRunner == runner)
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)

	body := newBM25InsertRequest("message")
	changed, ok, err := manager.TryMaterialize(1, 2, body)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, ok)
	require.True(t, HasFieldData(body.GetFieldsData(), 102))
	require.True(t, HasFieldData(body.GetFieldsData(), 104))

	latestBody := newBM25InsertRequest("latest message")
	changed, ok, err = manager.TryMaterialize(1, LatestFunctionRunnerVersion, latestBody)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, ok)
	require.True(t, HasFieldData(latestBody.GetFieldsData(), 102))
	require.True(t, HasFieldData(latestBody.GetFieldsData(), 104))
}

func TestFunctionRunnerManagerReleaseRemovesInitializingEntry(t *testing.T) {
	manager := newFunctionRunnerManager()
	t.Cleanup(manager.Close)

	schema := newBM25SignatureTestSchema()
	started := make(chan struct{})
	releaseBuild := make(chan struct{})
	var once sync.Once
	buildDone := make(chan struct{})
	patchBuildEmbeddingRunner(t, func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (FunctionRunner, error) {
		defer close(buildDone)
		once.Do(func() {
			close(started)
		})
		<-releaseBuild
		return newTestFunctionRunner(schema, fn)
	})

	require.NoError(t, manager.Alloc(1, "v1", schema))
	<-started

	manager.Release(1, "v1")
	requireFunctionRunnerEntryRemoved(t, manager, 1)

	close(releaseBuild)
	<-buildDone
}

func TestFunctionRunnerManagerReleaseCloseDoesNotBlockManager(t *testing.T) {
	manager := newFunctionRunnerManager()
	t.Cleanup(manager.Close)

	schema := newBM25SignatureTestSchema()
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	var buildCount atomic.Int32
	patchBuildEmbeddingRunner(t, func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (FunctionRunner, error) {
		runner, err := newTestFunctionRunner(schema, fn)
		if err != nil {
			return nil, err
		}
		if buildCount.Add(1) == 1 {
			runner.closeStarted = closeStarted
			runner.releaseClose = releaseClose
		}
		return runner, nil
	})

	require.NoError(t, manager.Alloc(1, "v1", schema))
	requireRunnerByOutput(t, manager, 1, schema.GetVersion(), 102)

	releaseDone := make(chan struct{})
	go func() {
		manager.Release(1, "v1")
		close(releaseDone)
	}()

	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		require.Fail(t, "timed out waiting for runner close")
	}

	allocErr := make(chan error, 1)
	go func() {
		allocErr <- manager.Alloc(2, "v2", schema)
	}()

	select {
	case err := <-allocErr:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.Fail(t, "manager was blocked by runner close")
	}

	select {
	case <-releaseDone:
		require.Fail(t, "release should still be waiting for runner close")
	default:
	}

	close(releaseClose)
	select {
	case <-releaseDone:
	case <-time.After(time.Second):
		require.Fail(t, "timed out waiting for release")
	}
}

func TestFunctionRunnerManagerRunWithRunnerProtectsConcurrentClose(t *testing.T) {
	manager := newFunctionRunnerManager()
	t.Cleanup(manager.Close)

	schema := newBM25SignatureTestSchema()
	closeStarted := make(chan struct{})
	// The manager may build (and immediately discard) more than one runner for
	// the same key under a concurrent re-init, and every built runner shares
	// this closeStarted channel. Guard the close with a shared Once so a second
	// runner's Close does not close an already-closed channel (which panicked
	// and made this test flaky under -race in ci-v2/build-ut-cov).
	var closeStartedOnce sync.Once
	patchBuildEmbeddingRunner(t, func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (FunctionRunner, error) {
		runner, err := newTestFunctionRunner(schema, fn)
		if err != nil {
			return nil, err
		}
		runner.closeStarted = closeStarted
		runner.closeStartedOnce = &closeStartedOnce
		return runner, nil
	})
	require.NoError(t, manager.Alloc(1, "v1", schema))

	runStarted := make(chan struct{})
	releaseRun := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		_, err := manager.RunWithRunner(context.Background(), 1, schema.GetVersion(), 102, func(functionType schemapb.FunctionType, runner FunctionRunner) error {
			if functionType != schemapb.FunctionType_BM25 {
				return errors.New("unexpected function type")
			}
			close(runStarted)
			<-releaseRun
			return nil
		})
		runDone <- err
	}()
	<-runStarted

	releaseDone := make(chan struct{})
	go func() {
		manager.Release(1, "v1")
		close(releaseDone)
	}()

	select {
	case <-closeStarted:
		require.Fail(t, "runner close started while callback was still running")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseRun)
	require.NoError(t, <-runDone)
	select {
	case <-releaseDone:
	case <-time.After(time.Second):
		require.Fail(t, "timed out waiting for release")
	}
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		require.Fail(t, "timed out waiting for runner close")
	}
}

func TestFunctionRunnerManagerRunWithAnalyzerUsesBM25Runner(t *testing.T) {
	manager, _ := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	schema := newBM25SignatureTestSchema()
	require.NoError(t, manager.Alloc(1, "v1", schema))

	var tokens [][]*milvuspb.AnalyzerToken
	ok, err := manager.RunWithAnalyzer(context.Background(), 1, LatestFunctionRunnerVersion, 101, func(analyzer Analyzer) error {
		var analyzeErr error
		tokens, analyzeErr = analyzer.BatchAnalyze(false, false, []string{"hello world"})
		return analyzeErr
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, tokens, 1)
	require.Equal(t, "hello world", tokens[0][0].GetToken())
}

func TestFunctionRunnerManagerRunWithRunnerLatestVersionUsesLatestSnapshot(t *testing.T) {
	manager, _ := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	base := newBM25SignatureTestSchema()
	require.NoError(t, manager.Alloc(1, "v1", base))

	changedOutput := newSchemaWithChangedOutput(base)
	require.NoError(t, manager.Update(1, "v1", changedOutput))

	ok, err := manager.RunWithRunner(context.Background(), 1, LatestFunctionRunnerVersion, 104, func(functionType schemapb.FunctionType, runner FunctionRunner) error {
		require.Equal(t, schemapb.FunctionType_BM25, functionType)
		require.Equal(t, int64(104), runner.GetOutputFields()[0].GetFieldID())
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)
}

func TestFunctionRunnerManagerSchemaVersionZeroIsExplicit(t *testing.T) {
	manager, _ := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	base := newBM25SignatureTestSchema()
	base.Version = 0
	require.NoError(t, manager.Alloc(1, "v0", base))

	changedOutput := newSchemaWithChangedOutput(base)
	changedOutput.Version = 1
	require.NoError(t, manager.Alloc(1, "v1", changedOutput))
	requireRunnerByOutput(t, manager, 1, base.GetVersion(), 102)
	requireRunnerByOutput(t, manager, 1, changedOutput.GetVersion(), 104)

	versionZeroBody := newBM25InsertRequest("version zero message")
	changed, ok, err := manager.TryMaterialize(1, 0, versionZeroBody)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, ok)
	require.True(t, HasFieldData(versionZeroBody.GetFieldsData(), 102))
	require.False(t, HasFieldData(versionZeroBody.GetFieldsData(), 104))

	latestBody := newBM25InsertRequest("latest message")
	changed, ok, err = manager.TryMaterialize(1, LatestFunctionRunnerVersion, latestBody)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, ok)
	require.False(t, HasFieldData(latestBody.GetFieldsData(), 102))
	require.True(t, HasFieldData(latestBody.GetFieldsData(), 104))
}

func TestFunctionRunnerManagerMaterializeRequiresAllocation(t *testing.T) {
	manager, _ := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	changed, err := manager.Materialize(context.Background(), 1, newBM25SignatureTestSchema(), newBM25InsertRequest("message"))
	require.ErrorContains(t, err, "not allocated")
	require.False(t, changed)
	requireFunctionRunnerEntryRemoved(t, manager, 1)
}

func TestFunctionRunnerManagerMaterializeNilSchemaUsesLatestSnapshot(t *testing.T) {
	manager, _ := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	base := newBM25SignatureTestSchema()
	require.NoError(t, manager.Alloc(1, "v1", base))

	changedOutput := newSchemaWithChangedOutput(base)
	require.NoError(t, manager.Update(1, "v1", changedOutput))

	body := newBM25InsertRequest("latest message")
	changed, err := manager.Materialize(context.Background(), 1, nil, body)
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, HasFieldData(body.GetFieldsData(), 102))
	require.True(t, HasFieldData(body.GetFieldsData(), 104))
}

func TestFunctionRunnerManagerMaterializeNoEmbeddingFunctionDoesNotRequireAllocation(t *testing.T) {
	manager := newFunctionRunnerManager()
	t.Cleanup(manager.Close)

	schema := cloneCollectionSchema(newBM25SignatureTestSchema())
	schema.Functions = nil

	changed, err := manager.Materialize(context.Background(), 1, schema, newBM25InsertRequest("message"))
	require.NoError(t, err)
	require.False(t, changed)
	requireFunctionRunnerEntryRemoved(t, manager, 1)
}

func TestFunctionRunnerManagerMaterializeNoRunnerBackedFunctionDoesNotRequireAllocation(t *testing.T) {
	manager := newFunctionRunnerManager()
	t.Cleanup(manager.Close)

	schema := cloneCollectionSchema(newBM25SignatureTestSchema())
	schema.Functions = []*schemapb.FunctionSchema{
		{
			Id:             200,
			Name:           "text_embedding",
			Type:           schemapb.FunctionType_TextEmbedding,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{103},
		},
	}

	changed, err := manager.Materialize(context.Background(), 1, schema, newBM25InsertRequest("message"))
	require.NoError(t, err)
	require.False(t, changed)
	requireFunctionRunnerEntryRemoved(t, manager, 1)
}

func TestFunctionRunnerManagerMaterializeHonorsContextDuringForegroundInit(t *testing.T) {
	manager := newFunctionRunnerManager()
	t.Cleanup(manager.Close)

	schema := newBM25SignatureTestSchema()
	signature := firstEmbeddingSignature(t, schema)

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	patchBuildEmbeddingRunner(t, func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (FunctionRunner, error) {
		once.Do(func() {
			close(started)
		})
		<-release
		return newTestFunctionRunner(schema, fn)
	})
	allocVChannelForTest(manager, 1, "v1", schema.GetVersion())

	firstErr := make(chan error, 1)
	go func() {
		_, err := manager.Materialize(context.Background(), 1, schema, newBM25InsertRequest("first"))
		firstErr <- err
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	changed, err := manager.Materialize(ctx, 1, schema, newBM25InsertRequest("second"))
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, changed)
	requireFunctionRunnerState(t, manager, 1, signature, functionRunnerStateInitializing)

	close(release)
	require.NoError(t, <-firstErr)
}

func TestFunctionRunnerManagerMaterializeSingleflightsInitialBuild(t *testing.T) {
	manager := newFunctionRunnerManager()
	t.Cleanup(manager.Close)

	schema := newBM25SignatureTestSchema()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var buildCount atomic.Int32
	patchBuildEmbeddingRunner(t, func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (FunctionRunner, error) {
		buildCount.Add(1)
		once.Do(func() {
			close(started)
		})
		<-release
		return newTestFunctionRunner(schema, fn)
	})
	allocVChannelForTest(manager, 1, "v1", schema.GetVersion())

	firstErr := make(chan error, 1)
	go func() {
		_, err := manager.Materialize(context.Background(), 1, schema, newBM25InsertRequest("first"))
		firstErr <- err
	}()
	<-started

	secondErr := make(chan error, 1)
	go func() {
		_, err := manager.Materialize(context.Background(), 1, schema, newBM25InsertRequest("second"))
		secondErr <- err
	}()

	close(release)
	require.NoError(t, <-firstErr)
	require.NoError(t, <-secondErr)
	require.Equal(t, int32(1), buildCount.Load())
}

func TestFunctionRunnerManagerRetriesFailedInitOnNextRequest(t *testing.T) {
	manager := newFunctionRunnerManager()
	t.Cleanup(manager.Close)

	schema := newBM25SignatureTestSchema()
	signature := firstEmbeddingSignature(t, schema)

	var buildCount atomic.Int32
	expectedErr := errors.New("mock init failed")
	patchBuildEmbeddingRunner(t, func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (FunctionRunner, error) {
		if buildCount.Add(1) == 1 {
			return nil, expectedErr
		}
		return newTestFunctionRunner(schema, fn)
	})
	allocVChannelForTest(manager, 1, "v1", schema.GetVersion())

	changed, err := manager.Materialize(context.Background(), 1, schema, newBM25InsertRequest("first"))
	require.ErrorIs(t, err, expectedErr)
	require.False(t, changed)
	requireFunctionRunnerState(t, manager, 1, signature, functionRunnerStateFailed)

	body := newBM25InsertRequest("second")
	changed, err = manager.Materialize(context.Background(), 1, schema, body)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, HasFieldData(body.GetFieldsData(), 102))
	require.Equal(t, int32(2), buildCount.Load())
	requireFunctionRunnerState(t, manager, 1, signature, functionRunnerStateReady)
}

func TestFunctionRunnerManagerAllocRetriesFailedInitOnNextRequest(t *testing.T) {
	manager := newFunctionRunnerManager()
	t.Cleanup(manager.Close)

	schema := newBM25SignatureTestSchema()
	signature := firstEmbeddingSignature(t, schema)

	var buildCount atomic.Int32
	expectedErr := errors.New("mock recover init failed")
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	patchBuildEmbeddingRunner(t, func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (FunctionRunner, error) {
		count := buildCount.Add(1)
		if count == 1 {
			once.Do(func() {
				close(started)
			})
			<-release
			return nil, expectedErr
		}
		return newTestFunctionRunner(schema, fn)
	})

	require.NoError(t, manager.Alloc(1, "v1", schema))
	<-started
	close(release)
	requireFunctionRunnerStateEventually(t, manager, 1, signature, functionRunnerStateFailed)

	body := newBM25InsertRequest("message")
	changed, err := manager.Materialize(context.Background(), 1, schema, body)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, HasFieldData(body.GetFieldsData(), 102))
	require.Equal(t, int32(2), buildCount.Load())
	requireFunctionRunnerState(t, manager, 1, signature, functionRunnerStateReady)
}

func TestFunctionRunnerManagerAllocDoesNotStartWhenReady(t *testing.T) {
	manager, factory := newMockFunctionRunnerManager(t)
	t.Cleanup(manager.Close)

	schema := newBM25SignatureTestSchema()
	require.NoError(t, manager.Alloc(1, "v1", schema))
	runner := requireRunnerByOutput(t, manager, 1, schema.GetVersion(), 102)

	require.NoError(t, manager.Alloc(1, "v1", schema))
	require.Equal(t, int32(1), factory.buildCount.Load())

	manager.Release(1, "v1")
	requireFunctionRunnerEntryRemoved(t, manager, 1)
	require.True(t, runner.isClosed())
}

func allocVChannelForTest(manager *functionRunnerManager, collectionID int64, vchannel string, schemaVersion int32) {
	entry := manager.getOrCreateEntry(collectionID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.keyVersions[vchannel] = schemaVersion
}

func functionRunnerEntrySnapshot(
	t *testing.T,
	manager *functionRunnerManager,
	collectionID int64,
) (map[string]int32, map[int32]struct{}, int) {
	t.Helper()

	manager.mu.RLock()
	entry := manager.entries[collectionID]
	manager.mu.RUnlock()
	require.NotNil(t, entry)

	entry.mu.RLock()
	defer entry.mu.RUnlock()

	keyVersions := make(map[string]int32, len(entry.keyVersions))
	for key, version := range entry.keyVersions {
		keyVersions[key] = version
	}
	versionRunners := make(map[int32]struct{}, len(entry.versionRunners))
	for version := range entry.versionRunners {
		versionRunners[version] = struct{}{}
	}
	return keyVersions, versionRunners, len(entry.runners)
}

func functionRunnerKeyVersionSnapshot(
	t *testing.T,
	manager *functionRunnerManager,
	collectionID int64,
	key string,
) int32 {
	t.Helper()

	manager.mu.RLock()
	entry := manager.entries[collectionID]
	manager.mu.RUnlock()
	require.NotNil(t, entry)

	entry.mu.RLock()
	defer entry.mu.RUnlock()

	version, ok := entry.keyVersions[key]
	require.True(t, ok)
	return version
}

func requireFunctionRunnerEntryRemoved(t *testing.T, manager *functionRunnerManager, collectionID int64) {
	t.Helper()

	manager.mu.RLock()
	defer manager.mu.RUnlock()
	require.NotContains(t, manager.entries, collectionID)
}

func requireFunctionRunnerState(
	t *testing.T,
	manager *functionRunnerManager,
	collectionID int64,
	signature string,
	state functionRunnerState,
) {
	t.Helper()

	manager.mu.RLock()
	entry := manager.entries[collectionID]
	manager.mu.RUnlock()
	require.NotNil(t, entry)

	entry.mu.RLock()
	runnerEntry := entry.runners[signature]
	entry.mu.RUnlock()
	require.NotNil(t, runnerEntry)

	runnerEntry.mu.RLock()
	defer runnerEntry.mu.RUnlock()
	require.Equal(t, state, runnerEntry.state)
}

func requireFunctionRunnerStateEventually(
	t *testing.T,
	manager *functionRunnerManager,
	collectionID int64,
	signature string,
	state functionRunnerState,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		manager.mu.RLock()
		entry := manager.entries[collectionID]
		manager.mu.RUnlock()
		if entry == nil {
			return false
		}

		entry.mu.RLock()
		runnerEntry := entry.runners[signature]
		entry.mu.RUnlock()
		if runnerEntry == nil {
			return false
		}

		runnerEntry.mu.RLock()
		defer runnerEntry.mu.RUnlock()
		return runnerEntry.state == state
	}, time.Second, 10*time.Millisecond)
}

func requireRunnerByOutput(
	t *testing.T,
	manager *functionRunnerManager,
	collectionID int64,
	schemaVersion int32,
	outputFieldID int64,
) *testFunctionRunner {
	t.Helper()

	var testRunner *testFunctionRunner
	ok, err := manager.RunWithRunner(context.Background(), collectionID, schemaVersion, outputFieldID, func(functionType schemapb.FunctionType, runner FunctionRunner) error {
		castRunner, ok := runner.(*testFunctionRunner)
		require.True(t, ok)
		testRunner = castRunner
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, testRunner)
	return testRunner
}

func firstEmbeddingSignature(t *testing.T, schema *schemapb.CollectionSchema) string {
	t.Helper()

	functions := embeddingFunctions(schema)
	require.NotEmpty(t, functions)
	signature, err := embeddingFunctionSignature(schema, functions[0])
	require.NoError(t, err)
	return signature
}

func newMockFunctionRunnerManager(t *testing.T) (*functionRunnerManager, *testFunctionRunnerFactory) {
	t.Helper()

	manager := newFunctionRunnerManager()
	factory := &testFunctionRunnerFactory{}
	patchBuildEmbeddingRunner(t, factory.Build)
	return manager, factory
}

func patchBuildEmbeddingRunner(
	t *testing.T,
	build func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (FunctionRunner, error),
) {
	t.Helper()

	mock := mockey.Mock(BuildEmbeddingRunner).To(build).Build()
	t.Cleanup(func() {
		mock.UnPatch()
	})
}

type testFunctionRunnerFactory struct {
	buildCount atomic.Int32
	mu         sync.Mutex
	runners    []*testFunctionRunner
}

func (f *testFunctionRunnerFactory) Build(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (FunctionRunner, error) {
	f.buildCount.Add(1)
	runner, err := newTestFunctionRunner(schema, fn)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.runners = append(f.runners, runner)
	return runner, nil
}

type testFunctionRunner struct {
	schema       *schemapb.FunctionSchema
	inputFields  []*schemapb.FieldSchema
	outputFields []*schemapb.FieldSchema
	closed       atomic.Int32
	closeOnce    sync.Once
	closeStarted chan struct{}
	// closeStartedOnce, when set, guards close(closeStarted) across runner
	// instances that share the same closeStarted channel. It falls back to the
	// per-instance closeOnce when nil.
	closeStartedOnce *sync.Once
	releaseClose     chan struct{}
}

func newTestFunctionRunner(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (*testFunctionRunner, error) {
	if len(fn.GetInputFieldIds()) != 1 {
		return nil, errors.New("test runner expects one input field")
	}
	inputField := typeutil.GetField(schema, fn.GetInputFieldIds()[0])
	if inputField == nil {
		return nil, errors.New("input field not found")
	}
	outputField := typeutil.GetFunctionOutputField(schema, fn)
	if outputField == nil {
		return nil, errors.New("output field not found")
	}

	return &testFunctionRunner{
		schema: proto.Clone(fn).(*schemapb.FunctionSchema),
		inputFields: []*schemapb.FieldSchema{
			proto.Clone(inputField).(*schemapb.FieldSchema),
		},
		outputFields: []*schemapb.FieldSchema{
			proto.Clone(outputField).(*schemapb.FieldSchema),
		},
	}, nil
}

func (r *testFunctionRunner) BatchRun(inputs ...any) ([]any, error) {
	rowCount := 0
	if len(inputs) > 0 {
		if values, ok := inputs[0].([]string); ok {
			rowCount = len(values)
		}
	}
	return []any{&schemapb.SparseFloatArray{
		Contents: make([][]byte, rowCount),
		Dim:      1,
	}}, nil
}

func (r *testFunctionRunner) BatchAnalyze(withDetail bool, withHash bool, inputs ...any) ([][]*milvuspb.AnalyzerToken, error) {
	if len(inputs) == 0 {
		return nil, errors.New("batch input is empty")
	}
	texts, ok := inputs[0].([]string)
	if !ok {
		return nil, errors.New("batch input not string list")
	}
	result := make([][]*milvuspb.AnalyzerToken, 0, len(texts))
	for _, text := range texts {
		result = append(result, []*milvuspb.AnalyzerToken{{Token: text}})
	}
	return result, nil
}

func (r *testFunctionRunner) GetSchema() *schemapb.FunctionSchema {
	return r.schema
}

func (r *testFunctionRunner) GetOutputFields() []*schemapb.FieldSchema {
	return r.outputFields
}

func (r *testFunctionRunner) GetInputFields() []*schemapb.FieldSchema {
	return r.inputFields
}

func (r *testFunctionRunner) Close() {
	if r.closeStarted != nil {
		markCloseStarted := func() { close(r.closeStarted) }
		if r.closeStartedOnce != nil {
			r.closeStartedOnce.Do(markCloseStarted)
		} else {
			r.closeOnce.Do(markCloseStarted)
		}
	}
	if r.releaseClose != nil {
		<-r.releaseClose
	}
	r.closed.CompareAndSwap(0, 1)
}

func (r *testFunctionRunner) isClosed() bool {
	return r.closed.Load() == 1
}

func newBM25SignatureTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:    "test",
		Version: 1,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: analyzerParams, Value: "{}"},
				},
			},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			{FieldID: 103, Name: "dense", DataType: schemapb.DataType_FloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Id:             100,
				Name:           "bm25",
				Type:           schemapb.FunctionType_BM25,
				InputFieldIds:  []int64{101},
				OutputFieldIds: []int64{102},
			},
		},
	}
}

func newSchemaWithChangedOutput(base *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	schema := cloneCollectionSchema(base)
	schema.Version = 2
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:          104,
		Name:             "sparse_v2",
		DataType:         schemapb.DataType_SparseFloatVector,
		IsFunctionOutput: true,
	})
	schema.Functions[0].OutputFieldIds = []int64{104}
	return schema
}

func newSchemaWithAddedFunction(base *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	schema := cloneCollectionSchema(base)
	schema.Version = 2
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:          104,
		Name:             "sparse_extra",
		DataType:         schemapb.DataType_SparseFloatVector,
		IsFunctionOutput: true,
	})
	schema.Functions = append(schema.Functions, &schemapb.FunctionSchema{
		Id:             101,
		Name:           "bm25_extra",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{104},
	})
	return schema
}

func cloneCollectionSchema(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	return proto.Clone(schema).(*schemapb.CollectionSchema)
}

func newBM25InsertRequest(texts ...string) *msgpb.InsertRequest {
	rowCount := len(texts)
	pks := make([]int64, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		pks = append(pks, int64(i+1))
	}

	return &msgpb.InsertRequest{
		FieldsData: []*schemapb.FieldData{
			{
				FieldId:   100,
				FieldName: "pk",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: pks},
						},
					},
				},
			},
			{
				FieldId:   101,
				FieldName: "text",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: texts},
						},
					},
				},
			},
		},
	}
}
