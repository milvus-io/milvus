/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package function

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The FunctionSchema is shared with the compaction plan and runner constructions
// can run concurrently (clustering builds one materializer per segment), so the
// constructor must keep the derived num_hashes runner-local and never write it
// back into the schema.
func TestMinHashRunnerDoesNotMutateSharedSchema(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "hash", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "64"},
		}},
	}}
	// num_hashes intentionally omitted: the runner derives it from the output dim.
	funSchema := &schemapb.FunctionSchema{
		Name:           "minhash",
		Type:           schemapb.FunctionType_MinHash,
		InputFieldIds:  []int64{100},
		OutputFieldIds: []int64{101},
	}

	runner, err := NewMinHashFunctionRunner(collSchema, funSchema)
	require.NoError(t, err)
	defer runner.Close()

	require.Empty(t, funSchema.GetParams(), "constructor must not write derived params into the shared schema")
	minHashRunner, ok := runner.(*MinHashFunctionRunner)
	require.True(t, ok)
	require.Equal(t, 2, minHashRunner.numHashes)
}

func TestMinHashRunnerValidatesExplicitNumHashesBeforeCreatingAnalyzer(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "hash", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "64"},
		}},
	}}
	funSchema := &schemapb.FunctionSchema{
		Name: "minhash", Type: schemapb.FunctionType_MinHash,
		InputFieldIds: []int64{100}, OutputFieldIds: []int64{101},
		Params: []*commonpb.KeyValuePair{{Key: NumHashesKey, Value: "4"}},
	}

	analyzerCalls := 0
	patch := mockey.Mock(analyzer.NewAnalyzer).To(func(string, string) (analyzer.Analyzer, error) {
		analyzerCalls++
		return nil, errors.New("unexpected analyzer construction")
	}).Build()
	defer patch.UnPatch()

	runner, err := NewMinHashFunctionRunner(collSchema, funSchema)
	require.Nil(t, runner)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	require.ErrorContains(t, err, "does not match expected dim")
	require.Zero(t, analyzerCalls)
}

func TestMinHashRunnerExplicitNumHashesMatchesOutputDim(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "hash", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "64"},
		}},
	}}
	funSchema := &schemapb.FunctionSchema{
		Name: "minhash", Type: schemapb.FunctionType_MinHash,
		InputFieldIds: []int64{100}, OutputFieldIds: []int64{101},
		Params: []*commonpb.KeyValuePair{{Key: NumHashesKey, Value: "2"}},
	}

	runner, err := NewMinHashFunctionRunner(collSchema, funSchema)
	require.NoError(t, err)
	defer runner.Close()
	require.Equal(t, 2, runner.(*MinHashFunctionRunner).numHashes)
}

// num_hashes = 2^59+1: (2^59+1)*32 wraps around int64 to 32, so a
// multiply-based dim check would accept it and pass the raw value to
// initializePermutations' slice allocation.
func TestMinHashRunnerRejectsNumHashesOverflowingDimCheck(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "hash", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "32"},
		}},
	}}
	funSchema := &schemapb.FunctionSchema{
		Name: "minhash", Type: schemapb.FunctionType_MinHash,
		InputFieldIds: []int64{100}, OutputFieldIds: []int64{101},
		Params: []*commonpb.KeyValuePair{{Key: NumHashesKey, Value: "576460752303423489"}},
	}

	analyzerCalls := 0
	patch := mockey.Mock(analyzer.NewAnalyzer).To(func(string, string) (analyzer.Analyzer, error) {
		analyzerCalls++
		return nil, errors.New("unexpected analyzer construction")
	}).Build()
	defer patch.UnPatch()

	runner, err := NewMinHashFunctionRunner(collSchema, funSchema)
	require.Nil(t, runner)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	require.ErrorContains(t, err, "does not match expected dim")
	require.Zero(t, analyzerCalls)
}

func TestValidateMinHashFunctionRejectsNumHashesOverflowingDimCheck(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{Name: "text", DataType: schemapb.DataType_VarChar},
		{Name: "hash", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "32"},
		}},
	}}
	funSchema := &schemapb.FunctionSchema{
		Name: "minhash", Type: schemapb.FunctionType_MinHash,
		InputFieldNames: []string{"text"}, OutputFieldNames: []string{"hash"},
		Params: []*commonpb.KeyValuePair{{Key: NumHashesKey, Value: "576460752303423489"}},
	}

	err := ValidateMinHashFunction(collSchema, funSchema)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.ErrorContains(t, err, "does not match expected dim")
}

func TestMinHashRunnerEmptyBatchKeepsSchemaDim(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "hash", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "64"},
		}},
	}}
	funSchema := &schemapb.FunctionSchema{
		Name: "minhash", Type: schemapb.FunctionType_MinHash,
		InputFieldIds: []int64{100}, OutputFieldIds: []int64{101},
	}

	runner, err := NewMinHashFunctionRunner(collSchema, funSchema)
	require.NoError(t, err)
	defer runner.Close()

	outputs, err := runner.BatchRun([]string{})
	require.NoError(t, err)
	require.Len(t, outputs, 1)
	fieldData := outputs[0].(*schemapb.FieldData)
	require.EqualValues(t, 64, fieldData.GetVectors().GetDim())
	require.Empty(t, fieldData.GetVectors().GetBinaryVector())
}
