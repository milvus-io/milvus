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

package rootcoord

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func validateSchemaEvolution(oldColl *model.Collection, newSchema *schemapb.CollectionSchema) error {
	var oldSchema *schemapb.CollectionSchema
	if oldColl != nil {
		oldSchema = oldColl.ToCollectionSchemaPB()
	}
	if err := schemautil.ValidateSchemaEvolution(oldSchema, newSchema); err != nil {
		return err
	}
	if err := typeutil.ValidateFuzzyBM25Functions(newSchema); err != nil {
		return err
	}
	return validateFuzzyBM25SchemaEvolution(oldSchema, newSchema)
}

func validateFuzzyBM25SchemaEvolution(oldSchema, newSchema *schemapb.CollectionSchema) error {
	oldFunctions := make(map[string]*schemapb.FunctionSchema, len(oldSchema.GetFunctions()))
	for _, function := range oldSchema.GetFunctions() {
		oldFunctions[function.GetName()] = function
	}
	for _, function := range newSchema.GetFunctions() {
		oldFunction := oldFunctions[function.GetName()]
		oldEnabled := typeutil.IsFuzzyEnabledBM25Function(oldFunction)
		newEnabled := typeutil.IsFuzzyEnabledBM25Function(function)
		if oldEnabled != newEnabled {
			return merr.WrapErrParameterInvalidMsg(
				"function param %q cannot be changed online for BM25 function %s", common.EnableFuzzyKey, function.GetName())
		}
	}
	return nil
}
