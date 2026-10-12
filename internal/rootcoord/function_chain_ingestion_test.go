// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestIngestionWheelResourcesFollowCollectionLifetime(t *testing.T) {
	resource := &internalpb.FileResourceInfo{Id: 7, Name: "tagger"}
	meta := &MetaTable{fileResourceID2Meta: map[int64]*internalpb.FileResourceInfo{7: resource}}
	core := &Core{meta: meta}
	op := &schemapb.FunctionChainOp{Op: "map", Expr: &schemapb.FunctionChainExpr{Name: "py_udf", Params: map[string]*schemapb.FunctionParamValue{"resource_name": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "tagger"}}}}}
	schema := &schemapb.CollectionSchema{FunctionChains: []*schemapb.FunctionChain{{Ops: []*schemapb.FunctionChainOp{op, op}}}}
	ids, err := core.validateSchemaAnalyzerFileResources(context.Background(), schema)
	require.NoError(t, err)
	require.Equal(t, []int64{7}, ids, "same wheel is referenced once")
	meta.fileResourceID2Meta = nil
	_, err = core.validateSchemaAnalyzerFileResources(context.Background(), schema)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	// A missing resource is diagnosed before reserveFileResourceRefs or WAL DDL.
	delete(op.Expr.Params, "resource_name")
	_, err = core.validateSchemaAnalyzerFileResources(context.Background(), schema)
	require.ErrorIs(t, err, merr.ErrParameterMissing)
}
