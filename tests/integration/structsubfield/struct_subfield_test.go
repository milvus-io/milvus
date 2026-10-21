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

package structsubfield

import (
	"context"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (s *StructSubFieldSuite) assertDirectChild(ctx context.Context, name string, pk int64, childName string) {
	resp := s.query(ctx, name, pk, structName+"["+childName+"]")
	s.Require().NotEmpty(resp.GetFieldsData())
	found := false
	for _, field := range resp.GetFieldsData() {
		if field.GetFieldName() == childName || field.GetFieldName() == structName+"["+childName+"]" {
			found = true
		}
		if field.GetFieldName() == structName {
			for _, child := range field.GetStructArrays().GetFields() {
				found = found || child.GetFieldName() == childName || child.GetFieldName() == structName+"["+childName+"]"
			}
		}
	}
	s.Require().True(found, "S[%s] missing from direct output_fields query", childName)
}

func (s *StructSubFieldSuite) TestDoubleHistoryGrowingSealedCompactionRestart() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	name := collectionName("history")
	s.create(ctx, baseSchema(name, true, true))
	s.Require().NoError(s.insert(ctx, name, 1, true)) // A: row predates the whole struct.
	s.addStruct(ctx, name, true)
	s.Require().NoError(s.insert(ctx, name, 2, true, baseChildren()...)) // B: two struct elements.
	emptyChildren := []*schemapb.FieldData{
		vectorChild("vec", &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}}}, true),
		scalarChild("ints", longRow(nil), true),
	}
	s.Require().NoError(s.insert(ctx, name, 5, true, emptyChildren...)) // Non-null empty struct array.
	s.flush(ctx, name)
	s.indexAndLoad(ctx, name)
	s.add(ctx, name, scalarSchema("c", schemapb.DataType_Int64, true))
	s.assertBackfilled(ctx, name, 1, "c", 0, false)
	s.assertBackfilled(ctx, name, 2, "c", 2, true)
	s.assertBackfilled(ctx, name, 5, "c", 0, true)
	s.assertDirectChild(ctx, name, 2, "c")

	s.Require().NoError(s.insert(ctx, name, 3, true, baseChildren()...)) // C, omitted c.
	explicit := append(baseChildren(), scalarChild("c", longRow([]int64{41}, true, false), true))
	s.Require().NoError(s.insert(ctx, name, 4, true, explicit...))
	s.assertBackfilled(ctx, name, 3, "c", 2, true)
	c := s.child(ctx, name, 4, "c")
	s.Require().Equal([]bool{true}, c.GetValidData())
	row := c.GetScalars().GetArrayData().GetData()[0]
	s.Require().Equal([]bool{true, false}, row.GetValidData())
	s.Require().Equal([]int64{41, 0}, row.GetLongData().GetData())

	s.flush(ctx, name)
	s.release(ctx, name)
	s.load(ctx, name)
	for _, pk := range []int64{2, 3} {
		s.assertBackfilled(ctx, name, pk, "c", 2, true)
	}
	s.assertBackfilled(ctx, name, 1, "c", 0, false)
	s.assertBackfilled(ctx, name, 5, "c", 0, true)
	s.compact(ctx, name)
	s.assertBackfilled(ctx, name, 1, "c", 0, false)
	s.assertBackfilled(ctx, name, 2, "c", 2, true)
	s.assertBackfilled(ctx, name, 5, "c", 0, true)
	s.restartQueryNode(ctx, name)
	s.assertBackfilled(ctx, name, 1, "c", 0, false)
	s.assertBackfilled(ctx, name, 2, "c", 2, true)
	s.assertBackfilled(ctx, name, 3, "c", 2, true)
	s.assertBackfilled(ctx, name, 5, "c", 0, true)
	s.Require().Equal([]bool{true, false}, s.child(ctx, name, 4, "c").GetScalars().GetArrayData().GetData()[0].GetValidData())
}

func (s *StructSubFieldSuite) TestAddedVectorAndNestedArrayWithMmap() {
	for _, tc := range []struct {
		name, child string
		field       *schemapb.FieldSchema
		mmap        bool
	}{
		{name: "vector", child: "new_vectors", field: vectorSchema("new_vectors", true)},
		{name: "nested_mmap", child: "nested", field: nestedSchema("nested"), mmap: true},
	} {
		s.Run(tc.name, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
			defer cancel()
			name := collectionName(tc.name)
			s.create(ctx, baseSchema(name, true, true))
			s.Require().NoError(s.insert(ctx, name, 1, true))
			s.addStruct(ctx, name, true)
			s.Require().NoError(s.insert(ctx, name, 2, true, baseChildren()...))
			s.flush(ctx, name)
			if tc.mmap {
				status, err := s.Cluster.MilvusClient.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
					CollectionName: name, Properties: []*commonpb.KeyValuePair{param(common.MmapEnabledKey, "true")},
				})
				s.Require().NoError(merr.CheckRPCCall(status, err))
			}
			s.indexAndLoad(ctx, name)
			s.add(ctx, name, tc.field)
			s.assertBackfilled(ctx, name, 1, tc.child, 0, false)
			s.assertBackfilled(ctx, name, 2, tc.child, 2, true)
			s.assertDirectChild(ctx, name, 2, tc.child)
			s.Require().NoError(s.insert(ctx, name, 3, true, baseChildren()...))
			s.assertBackfilled(ctx, name, 3, tc.child, 2, true)
			s.flush(ctx, name)
			s.release(ctx, name)
			s.load(ctx, name)
			s.assertBackfilled(ctx, name, 1, tc.child, 0, false)
			s.assertBackfilled(ctx, name, 2, tc.child, 2, true)
			s.compact(ctx, name)
			s.assertBackfilled(ctx, name, 2, tc.child, 2, true)
			s.restartQueryNode(ctx, name)
			s.assertBackfilled(ctx, name, 2, tc.child, 2, true)
		})
	}
}

func (s *StructSubFieldSuite) TestDropProviderMiddleAndLastGuards() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	name := collectionName("drop")
	schema := baseSchema(name, true, true, vectorSchema("vec", false), scalarSchema("mid", schemapb.DataType_Int64, false), scalarSchema("tail", schemapb.DataType_Int64, true))
	s.create(ctx, schema)
	children := []*schemapb.FieldData{
		baseChildren()[0],
		scalarChild("mid", longRow([]int64{11, 12}), true),
		scalarChild("tail", longRow([]int64{21}, true, false), true),
	}
	s.Require().NoError(s.insert(ctx, name, 1, true, children...))
	s.flush(ctx, name)
	s.indexAndLoad(ctx, name)
	s.drop(ctx, name, "S[vec]")
	s.Require().Equal([]int64{11, 12}, s.child(ctx, name, 1, "mid").GetScalars().GetArrayData().GetData()[0].GetLongData().GetData())
	s.Require().Equal([]bool{true, false}, s.child(ctx, name, 1, "tail").GetScalars().GetArrayData().GetData()[0].GetValidData())
	s.assertDirectChild(ctx, name, 1, "tail")
	for _, expression := range []string{"element_filter(S, $[tail] > 0)", "MATCH_ANY(S, $[tail] > 0)"} {
		resp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{CollectionName: name, Expr: expression, OutputFields: []string{"pk"}})
		s.Require().NoError(err)
		s.Require().ErrorContains(merr.Error(resp.GetStatus()), "not supported")
	}
	s.Require().ErrorContains(s.insert(ctx, name, 2, true, children...), `sub-field "vec" does not exist in struct field "S"`)
	s.Require().NoError(s.insert(ctx, name, 2, true, children[1:]...))
	s.flush(ctx, name)
	s.drop(ctx, name, "S[mid]")
	s.Require().ErrorContains(s.alterDrop(ctx, name, "S[tail]"), `cannot drop the last sub-field "S[tail]" of struct field "S"`)
	current := s.describe(ctx, name).GetSchema().GetStructArrayFields()[0].GetFields()
	s.Require().Len(current, 1)
	s.Require().Equal("tail", current[0].GetName())
	s.Require().Equal("S[tail]", s.describeInternal(ctx, name).GetSchema().GetStructArrayFields()[0].GetFields()[0].GetName())
	s.add(ctx, name, scalarSchema("fresh", schemapb.DataType_Int64, true))
	s.assertBackfilled(ctx, name, 1, "fresh", 2, true)
	s.compact(ctx, name)
	s.Require().Equal([]bool{true, false}, s.child(ctx, name, 1, "tail").GetScalars().GetArrayData().GetData()[0].GetValidData())
	s.restartQueryNode(ctx, name)
	s.Require().Equal([]bool{true, false}, s.child(ctx, name, 1, "tail").GetScalars().GetArrayData().GetData()[0].GetValidData())

	lastVector := collectionName("lastvec")
	s.create(ctx, baseSchema(lastVector, false, true, vectorSchema("vec", false), scalarSchema("mid", schemapb.DataType_Int64, false)))
	s.Require().ErrorContains(s.alterDrop(ctx, lastVector, "S[vec]"), "last vector field")
}

func (s *StructSubFieldSuite) TestDropAndReaddSameNameDifferentType() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	name := collectionName("readd")
	s.create(ctx, baseSchema(name, true, true, vectorSchema("vec", false), scalarSchema("ints", schemapb.DataType_Int64, true)))
	s.Require().NoError(s.insert(ctx, name, 1, true, baseChildren()...))
	s.add(ctx, name, scalarSchema("c", schemapb.DataType_Int64, true))
	s.Require().NoError(s.insert(ctx, name, 2, true, baseChildren()...))
	s.flush(ctx, name)
	s.drop(ctx, name, "S[c]")
	textField := scalarSchema("c", schemapb.DataType_VarChar, true)
	textField.TypeParams = append(textField.TypeParams, param(common.MaxLengthKey, "64"))
	s.add(ctx, name, textField)
	s.indexAndLoad(ctx, name)
	for _, pk := range []int64{1, 2} {
		s.assertBackfilled(ctx, name, pk, "c", 2, true)
	}
	s.Require().NoError(s.insert(ctx, name, 3, true,
		append(baseChildren(), scalarChild("c", stringRow([]string{"new"}, true, false), true))...))
	row := s.child(ctx, name, 3, "c").GetScalars().GetArrayData().GetData()[0]
	s.Require().Equal([]bool{true, false}, row.GetValidData())
	s.Require().Equal([]string{"new", ""}, row.GetStringData().GetData())
	s.flush(ctx, name)
	s.release(ctx, name)
	s.load(ctx, name)
	s.assertBackfilled(ctx, name, 1, "c", 2, true)
	s.Require().Equal([]bool{true, false}, s.child(ctx, name, 3, "c").GetScalars().GetArrayData().GetData()[0].GetValidData())
}

func (s *StructSubFieldSuite) TestNonNullableStructOmittedNewChild() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	name := collectionName("required_struct")
	s.create(ctx, baseSchema(name, true, false, vectorSchema("vec", false), scalarSchema("ints", schemapb.DataType_Int64, true)))
	s.Require().NoError(s.insert(ctx, name, 1, true, baseChildrenWithoutRowValidity()...))
	s.flush(ctx, name)
	s.add(ctx, name, scalarSchema("c", schemapb.DataType_Int64, true))
	s.indexAndLoad(ctx, name)
	s.Require().NoError(s.insert(ctx, name, 2, true, baseChildrenWithoutRowValidity()...))
	for _, pk := range []int64{1, 2} {
		s.assertBackfilled(ctx, name, pk, "c", 2, true, true)
	}
	s.flush(ctx, name)
	s.compact(ctx, name)
	s.assertBackfilled(ctx, name, 1, "c", 2, true, true)
	s.assertBackfilled(ctx, name, 2, "c", 2, true, true)
}

func (s *StructSubFieldSuite) TestStaleClientIncludingAllNullStructRejected() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	name := collectionName("stale")
	s.create(ctx, baseSchema(name, true, true, vectorSchema("vec", false), scalarSchema("ints", schemapb.DataType_Int64, true)))
	s.add(ctx, name, scalarSchema("c", schemapb.DataType_Int64, true))
	s.drop(ctx, name, "S[c]")
	stale := append(baseChildren(), scalarChild("c", longRow([]int64{7}, true, false), true))
	s.Require().ErrorContains(s.insert(ctx, name, 1, true, stale...), `sub-field "c" does not exist in struct field "S"`)
	nullChildren := []*schemapb.FieldData{
		vectorChild("vec", &schemapb.VectorField{Dim: dim}, false),
		scalarChild("ints", longRow(nil), false),
		scalarChild("c", longRow(nil), false),
	}
	for _, child := range nullChildren {
		if child.GetScalars() != nil {
			child.GetScalars().GetArrayData().Data = nil
		} else {
			child.GetVectors().GetVectorArray().Data = nil
		}
	}
	s.Require().ErrorContains(s.insert(ctx, name, 2, true, nullChildren...), `sub-field "c" does not exist in struct field "S"`)
}

func (s *StructSubFieldSuite) TestAlterSchemaAPIBoundariesAndAssignedIdentity() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	name := collectionName("api")
	s.create(ctx, baseSchema(name, true, true, vectorSchema("vec", false), scalarSchema("ints", schemapb.DataType_Int64, true)))
	for _, tc := range []struct {
		name, path, index, want string
		change                  func(*schemapb.FieldSchema)
		fn                      *schemapb.FunctionSchema
	}{
		{name: "missing_struct", path: "missing", want: `struct field "missing" not found`},
		{name: "nested_path", path: "S[inner]", want: `nested struct is not supported: struct_path "S[inner]" must be a single struct field name`},
		{name: "non_element_nullable", path: "S", want: `added sub-field "c" of struct field "S" must set element_nullable=true`, change: func(f *schemapb.FieldSchema) { f.ElementNullable = false }},
		{name: "default", path: "S", want: "default value is not supported for struct field, field name = c", change: func(f *schemapb.FieldSchema) {
			f.DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 1}}
		}},
		{name: "index", path: "S", index: "idx", want: "binding an index while adding a struct sub-field is not supported yet"},
		{name: "function", path: "S", fn: &schemapb.FunctionSchema{Name: "fn"}, want: "struct_path cannot be combined with a function"},
	} {
		s.Run(tc.name, func() {
			field := scalarSchema("c", schemapb.DataType_Int64, true)
			if tc.change != nil {
				tc.change(field)
			}
			s.Require().ErrorContains(s.alterAdd(ctx, name, tc.path, tc.index, field, tc.fn), tc.want)
		})
	}
	before := s.describe(ctx, name).GetSchema()
	maxID := int64(0)
	for _, field := range before.GetFields() {
		if field.GetFieldID() > maxID {
			maxID = field.GetFieldID()
		}
	}
	for _, parent := range before.GetStructArrayFields() {
		if parent.GetFieldID() > maxID {
			maxID = parent.GetFieldID()
		}
		for _, child := range parent.GetFields() {
			if child.GetFieldID() > maxID {
				maxID = child.GetFieldID()
			}
		}
	}
	field := scalarSchema("c", schemapb.DataType_Int64, true)
	field.FieldID = 99999 // The server must ignore client supplied IDs.
	s.add(ctx, name, field)
	after := s.describe(ctx, name).GetSchema()
	s.Require().Equal(before.GetVersion()+1, after.GetVersion())
	children := after.GetStructArrayFields()[0].GetFields()
	s.Require().Equal("c", children[len(children)-1].GetName())
	storedChildren := s.describeInternal(ctx, name).GetSchema().GetStructArrayFields()[0].GetFields()
	s.Require().Equal("S[c]", storedChildren[len(storedChildren)-1].GetName())
	s.Require().Equal(children[len(children)-1].GetFieldID(), storedChildren[len(storedChildren)-1].GetFieldID())
	s.Require().Greater(children[len(children)-1].GetFieldID(), maxID)
	s.Require().NotEqual(int64(99999), children[len(children)-1].GetFieldID())
	s.Require().ErrorContains(s.alterDrop(ctx, name, "c"), "field not found: c")
	beforeDrop := after.GetVersion()
	resp, err := s.Cluster.MilvusClient.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		CollectionName: name,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{
			DropRequest: &milvuspb.AlterCollectionSchemaRequest_DropRequest{
				Identifier: &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldId{FieldId: children[len(children)-1].GetFieldID()},
			},
		}},
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetAlterStatus(), err))
	s.waitVersion(ctx, name, beforeDrop+1)
	s.Require().Len(s.describe(ctx, name).GetSchema().GetStructArrayFields()[0].GetFields(), len(children)-1)
}

func (s *StructSubFieldSuite) TestLoadFieldsBeforeAndAfterReload() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	name := collectionName("loadfields")
	s.create(ctx, baseSchema(name, true, true, vectorSchema("vec", false), scalarSchema("ints", schemapb.DataType_Int64, true)))
	s.Require().NoError(s.insert(ctx, name, 1, true, baseChildren()...))
	s.flush(ctx, name)
	s.indexAndLoad(ctx, name, "pk", "embedding", structName)
	s.add(ctx, name, scalarSchema("c", schemapb.DataType_Int64, true))
	s.assertBackfilled(ctx, name, 1, "c", 2, true)
	s.assertDirectChild(ctx, name, 1, "c")
	s.Require().NoError(s.insert(ctx, name, 2, true, baseChildren()...))
	s.assertBackfilled(ctx, name, 2, "c", 2, true)
	s.assertDirectChild(ctx, name, 2, "c")
	s.release(ctx, name)
	s.load(ctx, name, "pk", "embedding", structName)
	s.assertBackfilled(ctx, name, 1, "c", 2, true)
	s.assertDirectChild(ctx, name, 1, "c")
	s.assertBackfilled(ctx, name, 2, "c", 2, true)
	s.assertDirectChild(ctx, name, 2, "c")
}

func (s *StructSubFieldSuite) TestBulkImportStillRejected() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	name := collectionName("import")
	s.create(ctx, baseSchema(name, true, true, vectorSchema("vec", false), scalarSchema("ints", schemapb.DataType_Int64, false)))
	s.add(ctx, name, scalarSchema("c", schemapb.DataType_Int64, true))
	resp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: name, Files: []*internalpb.ImportFile{{Paths: []string{"unused.json"}}},
	})
	s.Require().NoError(err)
	s.Require().ErrorContains(merr.Error(resp.GetStatus()), "bulk import of element-nullable or nested Array field S.c is not supported yet")
}
