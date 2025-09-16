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

package storagecommon

import (
	"fmt"
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	// column group id for short columns
	DefaultShortColumnGroupID = 0
)

type ColumnGroup struct {
	GroupID typeutil.UniqueID
	Columns []int // column indices
	Fields  []int64
}

func (cg ColumnGroup) String() string {
	return fmt.Sprintf("[GroupID: %d, ColumnIndices: %v, Fields: %v]", cg.GroupID, cg.Columns, cg.Fields)
}

func SplitColumns(fields []*schemapb.FieldSchema, stats map[int64]ColumnStats, policies ...ColumnGroupSplitPolicy) []ColumnGroup {
	split := newCurrentSplit(fields, stats)
	for _, policy := range policies {
		split = policy.Split(split)
	}
	sort.Slice(split.outputGroups, func(i, j int) bool {
		return split.outputGroups[i].GroupID < split.outputGroups[j].GroupID
	})
	return split.outputGroups
}

func DefaultPolicies() []ColumnGroupSplitPolicy {
	paramtable.Init()
	result := make([]ColumnGroupSplitPolicy, 0, 4)
	if paramtable.Get().CommonCfg.Stv2SplitSystemColumn.GetAsBool() {
		result = append(result, NewSystemColumnPolicy(paramtable.Get().CommonCfg.Stv2SystemColumnIncludePK.GetAsBool(),
			paramtable.Get().CommonCfg.Stv2SystemColumnIncludePartitionKey.GetAsBool(),
			paramtable.Get().CommonCfg.Stv2SystemColumnIncludeClusteringKey.GetAsBool()))
	}
	if paramtable.Get().CommonCfg.Stv2SplitByAvgSize.GetAsBool() {
		result = append(result, NewAvgSizePolicy(paramtable.Get().CommonCfg.Stv2SplitAvgSizeThreshold.GetAsInt64()))
	}
	result = append(result,
		NewSelectedDataTypePolicy(),
		NewRemanentShortPolicy(-1))
	return result
}

func IsVectorDataType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_BinaryVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector,
		schemapb.DataType_FloatVector,
		schemapb.DataType_SparseFloatVector,
		schemapb.DataType_ArrayOfVector:
		return true
	}
	return false
}
