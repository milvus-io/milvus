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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// num_hashes = 2^59+1: (2^59+1)*32 wraps around int64 to 32, so a
// multiply-based dim check would accept it and pass the raw value to
// initializePermutations' slice allocation on first use.
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
