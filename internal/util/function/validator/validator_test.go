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
