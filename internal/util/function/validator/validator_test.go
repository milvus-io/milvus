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

package validator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func minHashCollectionSchema(numHashes string) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 101, Name: "hash", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: "dim", Value: "64"},
			}},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:             "minhash",
			Type:             schemapb.FunctionType_MinHash,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"hash"},
			Params:           []*commonpb.KeyValuePair{{Key: function.NumHashesKey, Value: numHashes}},
		}},
	}
}

// The direct RootCoord DDL path calls ValidateFunction with
// disableRuntimeCheck=true; MinHash parameter validation is pure schema
// checking and must still run there.
func TestValidateFunctionChecksMinHashParamsWithRuntimeCheckDisabled(t *testing.T) {
	err := ValidateFunction(minHashCollectionSchema("3"), "minhash", true)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.ErrorContains(t, err, "does not match expected dim")
}

func TestValidateFunctionAcceptsValidMinHashWithRuntimeCheckDisabled(t *testing.T) {
	require.NoError(t, ValidateFunction(minHashCollectionSchema("2"), "minhash", true))
}

// legacyTextMinHashSchema holds a persisted pre-tightening MinHash function
// with a TEXT input next to a fresh, valid BM25 function named "new_bm25".
func legacyTextMinHashSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "legacy_text", DataType: schemapb.DataType_Text},
			{FieldID: 101, Name: "legacy_hash", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: "dim", Value: "64"},
			}},
			{FieldID: 102, Name: "doc", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
				{Key: "enable_analyzer", Value: "true"},
			}},
			{FieldID: 103, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:             "legacy_minhash",
				Type:             schemapb.FunctionType_MinHash,
				InputFieldNames:  []string{"legacy_text"},
				OutputFieldNames: []string{"legacy_hash"},
				Params:           []*commonpb.KeyValuePair{{Key: function.NumHashesKey, Value: "2"}},
			},
			{
				Name:             "new_bm25",
				Type:             schemapb.FunctionType_BM25,
				InputFieldNames:  []string{"doc"},
				OutputFieldNames: []string{"sparse"},
			},
		},
	}
}

// A legacy persisted function must not be re-judged by current-version rules
// when an unrelated function is validated on the targeted (add/alter) path.
func TestValidateFunctionGrandfathersLegacyFunctionsOnTargetedPath(t *testing.T) {
	require.NoError(t, ValidateFunction(legacyTextMinHashSchema(), "new_bm25", true))
}

// The targeted path still judges the target function itself.
func TestValidateFunctionTargetedStillJudgesTargetFunction(t *testing.T) {
	schema := legacyTextMinHashSchema()
	err := ValidateFunction(schema, "legacy_minhash", true)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.ErrorContains(t, err, "must be a VARCHAR field")
}

// Full validation (create_collection, needValidateFunctionName == "") keeps
// judging every function; nothing is grandfathered at creation.
func TestValidateFunctionFullValidationJudgesEveryFunction(t *testing.T) {
	err := ValidateFunction(legacyTextMinHashSchema(), "", true)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.ErrorContains(t, err, "must be a VARCHAR field")
}

// Cross-function invariants still see the full schema on the targeted path: a
// target function reusing a legacy function's output field is rejected.
func TestValidateFunctionTargetedKeepsCrossFunctionInvariants(t *testing.T) {
	schema := legacyTextMinHashSchema()
	schema.Functions[1].OutputFieldNames = []string{"legacy_hash"}
	err := ValidateFunction(schema, "new_bm25", true)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.ErrorContains(t, err, "duplicate function output field")
}
