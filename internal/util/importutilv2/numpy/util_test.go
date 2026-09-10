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

package numpy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func schemaWithFields(names ...string) *schemapb.CollectionSchema {
	fields := make([]*schemapb.FieldSchema, 0, len(names))
	for i, name := range names {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID: int64(100 + i), Name: name, DataType: schemapb.DataType_Int64,
		})
	}
	return &schemapb.CollectionSchema{Fields: fields}
}

// SourcePaths is what keeps a caller that inspects an import before reading it
// from being stricter than CreateReaders. It must drop exactly what CreateReaders
// drops -- no more, no less.
func TestSourcePaths(t *testing.T) {
	t.Run("drops paths naming no field", func(t *testing.T) {
		got := SourcePaths(schemaWithFields("pk", "vec"),
			[]string{"a/pk.npy", "a/vec.npy", "a/README.npy"})
		assert.ElementsMatch(t, []string{"a/pk.npy", "a/vec.npy"}, got)
	})

	t.Run("keeps the dynamic field", func(t *testing.T) {
		got := SourcePaths(schemaWithFields("pk", common.MetaFieldName),
			[]string{"a/pk.npy", "a/" + common.MetaFieldName + ".npy"})
		assert.ElementsMatch(t,
			[]string{"a/pk.npy", "a/" + common.MetaFieldName + ".npy"}, got)
	})

	t.Run("same basename in two directories collapses to one path", func(t *testing.T) {
		// CreateReaders keys its reader set by basename, so only one of these ever
		// opens. Returning both here would make sizing read a file the reader does
		// not, which is the asymmetry this helper exists to remove.
		got := SourcePaths(schemaWithFields("vec"), []string{"a/vec.npy", "b/vec.npy"})
		assert.Len(t, got, 1)
		assert.Contains(t, []string{"a/vec.npy", "b/vec.npy"}, got[0])
	})

	t.Run("empty inputs", func(t *testing.T) {
		assert.Empty(t, SourcePaths(schemaWithFields("vec"), nil))
		assert.Empty(t, SourcePaths(&schemapb.CollectionSchema{}, []string{"a/vec.npy"}))
	})
}
