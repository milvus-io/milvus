// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package importutilv2

// Derived Import V3 execution inputs. Both derivations are pure functions of the
// frozen collection schema (plus the backup flag), so DataCoord (slot sizing,
// sort-compaction planning) and the DataNode (reshard / import execution, sort
// compaction) compute them from the same helper instead of carrying a copy in a
// task plan, which would be a second source of truth.

import (
	"math"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SortFieldIDs returns the collection's storage sort order: the partition key
// first for namespace collections, the primary key always. Sort compaction and
// Import V3 must produce identically-ordered output, so both derive the order
// here rather than one sending it to the other.
func SortFieldIDs(schema *schemapb.CollectionSchema) ([]int64, error) {
	pk, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	if !schema.GetEnableNamespace() {
		return []int64{pk.GetFieldID()}, nil
	}
	partitionKey, err := typeutil.GetPartitionKeyFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	return []int64{partitionKey.GetFieldID(), pk.GetFieldID()}, nil
}

// FragmentSchema returns the schema of the fields physically present in Import
// V3 fragments and merge intermediates. Ordinary fragments carry the user fields
// plus a materialized RowID (Reshard computes it) and every function output
// column; backup fragments retain their source-provided function outputs, so
// they use the full system field schema.
//
// It is derived from the frozen collection schema, on the DataNode at dispatch
// and on the DataCoord for slot sizing, rather than frozen into the task plan.
func FragmentSchema(schema *schemapb.CollectionSchema, backup bool) *schemapb.CollectionSchema {
	cloned := proto.Clone(schema).(*schemapb.CollectionSchema)
	// Fragments and intermediates store TEXT as raw UTF-8, not manifest LOB
	// references, so TEXT is mapped to VarChar for the ordinary storage
	// sort/merge/writer paths. The max_length is a placeholder that nothing on
	// this path reads: TEXT has no length limit by design (insert skips the
	// check for TEXT), and the fragment sort/merge/writer never consults
	// max_length, so any value works. A sentinel avoids ever rejecting a valid
	// TEXT value if a later change starts validating max_length.
	for _, field := range cloned.GetFields() {
		if field.GetDataType() == schemapb.DataType_Text {
			field.DataType = schemapb.DataType_VarChar
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
				Key:   common.MaxLengthKey,
				Value: strconv.FormatInt(math.MaxInt64, 10),
			})
		}
	}
	if backup {
		return typeutil.AppendSystemFields(cloned)
	}
	cloned.Fields = append(cloned.Fields, &schemapb.FieldSchema{
		FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64,
	})
	return cloned
}
