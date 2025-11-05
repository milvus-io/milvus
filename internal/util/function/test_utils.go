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
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func MockMinHashFunctionSchema(
	collSchema *schemapb.CollectionSchema,
) error {
	// Find input and output fields
	var outputField *schemapb.FieldSchema
	var minHashFuncSchema *schemapb.FunctionSchema
	minHashFuncSchema = nil
	for _, fSchema := range collSchema.GetFunctions() {
		if fSchema.GetType() == schemapb.FunctionType_MinHash {
			minHashFuncSchema = fSchema
			break
		}
	}
	if minHashFuncSchema == nil {
		return nil
	} else {
		outputFieldId := minHashFuncSchema.GetOutputFieldIds()[0]
		for _, field := range collSchema.GetFields() {
			if field.GetFieldID() == outputFieldId {
				outputField = field
				break
			}
		}
	}
	var outputDim int64 = -1
	if outputField.GetDataType() != schemapb.DataType_BinaryVector {
		return fmt.Errorf("minhash function output field  is not binary vector type")
	}
	for _, param := range outputField.GetTypeParams() {
		if param.GetKey() == "dim" {
			val, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if err == nil {
				outputDim = val
				break
			}
		}
	}
	numHashes := int(outputDim / 32)
	seed := int64(1234)
	minHashFuncSchema.Params = append(minHashFuncSchema.Params, &commonpb.KeyValuePair{
		Key:   NumHashesKey,
		Value: strconv.Itoa(numHashes),
	})

	permA, permB := InitPermutations(numHashes, seed)
	permsEncoded := SerializePermutations(permA, permB)
	minHashFuncSchema.Params = append(minHashFuncSchema.Params, &commonpb.KeyValuePair{
		Key:   PermutationsKey,
		Value: permsEncoded,
	})
	return nil
}
