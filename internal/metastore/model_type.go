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
	"fmt"
	"reflect"
	"strings"
)

// ModelType describes one record type of the generic catalog: its name, its
// place in the key tree, and how a value of it is identified and encoded.
// Values are the type's existing Go structs (for example
// *model.SegmentChangeGroup); they carry nothing for the catalog.
//
// Records form a tree: a record sits at Root/c1/.../cn, where c1...cn is its
// coordinate, outermost first, and n is Depth. Every record of a type has
// exactly Depth components, so the levels above are directories. A prefix is
// the first k < Depth components; selecting a later component without an
// earlier one (Root/a/*/c) is not a prefix: list the common prefix and narrow
// it with ListOptions.Filter.
//
// Each type declares one registered *ModelType (see RegisterModelType) and
// names itself by it in catalog calls.
type ModelType struct {
	// Root is the type's directory below the catalog namespace, "/"-separated
	// without a leading or trailing "/".
	Root string
	// Depth is the number of coordinate components of a record, at least 1.
	Depth int
	// GoType is the Go type of the values, for example
	// reflect.TypeOf((*model.SegmentChangeGroup)(nil)). The catalog finds the
	// ModelType of a value written in an UpdateAction by it.
	GoType reflect.Type
	// Coordinate returns the coordinate of v, a non-nil value of GoType. It
	// reads only the identity fields, so it costs no encoding.
	Coordinate func(v any) []string
	// Marshal encodes v, a non-nil value of GoType.
	Marshal func(v any) ([]byte, error)
	// Unmarshal decodes a stored value into a new value of GoType. The
	// catalog checks that the decoded value's coordinate matches the one it
	// was stored under.
	Unmarshal func(value []byte) (any, error)
}

// The registry is written only by RegisterModelType, from package variable
// initializers, and read-only afterwards.
var modelTypesByGoType = map[reflect.Type]*ModelType{}

// RegisterModelType adds t to the registry and returns the registered type,
// the handle catalog calls take. Call it from a package variable
// initializer. It panics on an incomplete type, a malformed root, a Go type
// registered twice, or a root that overlaps a registered one.
func RegisterModelType(t ModelType) *ModelType {
	switch {
	case t.GoType == nil || t.Coordinate == nil || t.Marshal == nil || t.Unmarshal == nil:
		panic(fmt.Sprintf("metastore: model type %q is incomplete", t.Root))
	case t.Root == "" || strings.HasPrefix(t.Root, "/") || strings.HasSuffix(t.Root, "/") ||
		strings.Contains(t.Root, "//"):
		panic(fmt.Sprintf("metastore: model type root %q is malformed", t.Root))
	case t.Depth < 1:
		panic(fmt.Sprintf("metastore: model type %q depth %d is below 1", t.Root, t.Depth))
	case modelTypesByGoType[t.GoType] != nil:
		panic(fmt.Sprintf("metastore: Go type %v is registered twice", t.GoType))
	}
	// Compare directories, so "datacoord-meta/s" does not overlap
	// "datacoord-meta/stats-task".
	for _, other := range modelTypesByGoType {
		if a, b := t.Root+"/", other.Root+"/"; strings.HasPrefix(a, b) || strings.HasPrefix(b, a) {
			panic(fmt.Sprintf("metastore: model type root %q overlaps root %q", t.Root, other.Root))
		}
	}
	checkModelType(t)
	registered := &t
	modelTypesByGoType[t.GoType] = registered
	return registered
}

// checkModelType runs t's functions on an empty value of GoType, so a
// GoType the functions do not assert, or a coordinate of the wrong depth,
// panics at registration instead of at the first write.
func checkModelType(t ModelType) {
	if t.GoType.Kind() != reflect.Pointer {
		panic(fmt.Sprintf("metastore: model type %q Go type %v is not a pointer", t.Root, t.GoType))
	}
	defer func() {
		if r := recover(); r != nil {
			panic(fmt.Sprintf("metastore: model type %q does not handle its Go type %v: %v", t.Root, t.GoType, r))
		}
	}()
	empty := reflect.New(t.GoType.Elem()).Interface()
	if coord := t.Coordinate(empty); len(coord) != t.Depth {
		panic(fmt.Sprintf("coordinate %q has %d components, want %d", coord, len(coord), t.Depth))
	}
	_, _ = t.Marshal(empty)
}

// Registered reports whether t is a type RegisterModelType returned.
func (t *ModelType) Registered() bool {
	return t != nil && modelTypesByGoType[t.GoType] == t
}

// ModelTypeOf returns the registered type of value v's Go type.
func ModelTypeOf(v any) (*ModelType, bool) {
	t, ok := modelTypesByGoType[reflect.TypeOf(v)]
	return t, ok
}
