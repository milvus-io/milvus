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

package querymeter

import (
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// PredicateFieldIDs returns one entry per column reference in the planned
// predicate. Both sides of a boolean expression count even if the executor
// can short-circuit, and distinct references to the same field are retained.
func PredicateFieldIDs(expr *planpb.Expr) []int64 {
	return appendPredicateFieldIDs(nil, expr)
}

func appendPredicateFieldIDs(ids []int64, expr *planpb.Expr) []int64 {
	if expr == nil {
		return ids
	}
	node := expr.ProtoReflect()
	variant := node.WhichOneof(node.Descriptor().Oneofs().ByName("expr"))
	if variant == nil {
		return ids
	}
	// Descend into expression operands only. In particular, a TERM expression
	// can carry a large list of literal values unrelated to field accounting.
	node.Get(variant).Message().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Message() == nil {
			return true
		}
		if field.IsList() {
			if field.Message().FullName() == node.Descriptor().FullName() {
				list := value.List()
				for i := 0; i < list.Len(); i++ {
					ids = appendPredicateFieldIDs(ids, list.Get(i).Message().Interface().(*planpb.Expr))
				}
			}
			return true
		}
		switch message := value.Message().Interface().(type) {
		case *planpb.ColumnInfo:
			if message != nil {
				ids = append(ids, message.GetFieldId())
			}
		case *planpb.Expr:
			ids = appendPredicateFieldIDs(ids, message)
		}
		return true
	})
	return ids
}
