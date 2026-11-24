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

package metastore

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestModelTypeRegistry(t *testing.T) {
	got, ok := ModelTypeOf(&model.SegmentChangeGroup{})
	require.True(t, ok)
	require.Same(t, SegmentGroupType, got)
	got, ok = ModelTypeOf(&indexpb.AnalyzeTask{})
	require.True(t, ok)
	require.Same(t, AnalyzeTaskType, got)
	_, ok = ModelTypeOf(model.SegmentChangeGroup{})
	require.False(t, ok, "only the registered Go type, a pointer, matches")

	require.True(t, SegmentGroupType.Registered())
	copied := *SegmentGroupType
	require.False(t, (&copied).Registered(), "a copy was not returned by RegisterModelType")
	require.False(t, (*ModelType)(nil).Registered())
}

func TestRegisterModelType_Rejects(t *testing.T) {
	type probe struct{}
	valid := func(root string, depth int) ModelType {
		return ModelType{
			Root:       root,
			Depth:      depth,
			GoType:     reflect.TypeOf((*probe)(nil)),
			Coordinate: func(v any) []string { _ = v.(*probe); return []string{"1"} },
			Marshal:    func(any) ([]byte, error) { return nil, nil },
			Unmarshal:  func([]byte) (any, error) { return &probe{}, nil },
		}
	}
	incomplete := valid("probe", 1)
	incomplete.Marshal = nil
	duplicate := valid("probe", 1)
	duplicate.GoType = SegmentGroupType.GoType
	notPointer := valid("probe", 1)
	notPointer.GoType = reflect.TypeOf(probe{})
	mismatched := valid("probe", 1)
	mismatched.GoType = reflect.TypeOf((*int)(nil))
	tests := []struct {
		name string
		t    ModelType
	}{
		{"incomplete", incomplete},
		{"empty root", valid("", 1)},
		{"absolute root", valid("/probe", 1)},
		{"trailing slash", valid("probe/", 1)},
		{"empty component", valid("a//b", 1)},
		{"zero depth", valid("probe", 0)},
		{"Go type registered twice", duplicate},
		{"Go type not a pointer", notPointer},
		{"functions of another Go type", mismatched},
		{"coordinate of the wrong depth", valid("probe", 2)},
		{"inside another root", valid(SegmentGroupType.Root+"/x", 1)},
		{"contains another root", valid("datacoord-meta", 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Panics(t, func() { RegisterModelType(tt.t) })
		})
	}
}
