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
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ColumnStats contains sampled insert data statistics data
// pass this struct avoiding pass storage.InsertData to solve cycle import
type ColumnStats struct {
	MaxSize int64
	AvgSize int64
}

type currentSplit struct {
	// input
	fields []*schemapb.FieldSchema
	stats  map[int64]ColumnStats

	nextGroupID   int64
	outputGroups  []ColumnGroup
	processFields typeutil.Set[int64]
}

func newCurrentSplit(fields []*schemapb.FieldSchema, stats map[int64]ColumnStats) *currentSplit {
	return &currentSplit{
		fields:        fields,
		stats:         stats,
		processFields: typeutil.NewSet[int64](),
	}
}

func (c *currentSplit) SplitFields(groupID int64, fields []int64, indices []int) {
	c.processFields.Insert(fields...)
	c.outputGroups = append(c.outputGroups, ColumnGroup{Columns: indices, GroupID: groupID, Fields: fields})
}

func (c *currentSplit) NextGroupID() int64 {
	r := c.nextGroupID
	c.nextGroupID++
	return r
}

func (c *currentSplit) Processed(field int64) bool {
	return c.processFields.Contain(field)
}

func (c *currentSplit) Range(f func(idx int, field *schemapb.FieldSchema)) {
	for idx, field := range c.fields {
		if c.Processed(field.GetFieldID()) {
			continue
		}
		f(idx, field)
	}
}

// ColumnGroupSplitPolicy interface for column group split policy.
type ColumnGroupSplitPolicy interface {
	Split(currentSplit *currentSplit) *currentSplit
}

// selectedDataTypePolicy split widt datatype (vector, text) to a new column group.
type selectedDataTypePolicy struct{}

func (p *selectedDataTypePolicy) Split(currentSplit *currentSplit) *currentSplit {
	currentSplit.Range(func(idx int, field *schemapb.FieldSchema) {
		if IsVectorDataType(field.DataType) || field.DataType == schemapb.DataType_Text {
			currentSplit.SplitFields(field.GetFieldID(), []int64{field.GetFieldID()}, []int{idx})
		}
	})

	return currentSplit
}

func NewSelectedDataTypePolicy() ColumnGroupSplitPolicy {
	return &selectedDataTypePolicy{}
}

// systemColumnPolicy split system columns to a new column group
// if includePK is true, system columns include primary key column.
type systemColumnPolicy struct {
	includePrimaryKey    bool
	includePartitionKey  bool
	includeClusteringKey bool
}

func NewSystemColumnPolicy(includePK bool, includePartKey bool, includeClusteringKey bool) ColumnGroupSplitPolicy {
	return &systemColumnPolicy{
		includePrimaryKey:    includePK,
		includePartitionKey:  includePartKey,
		includeClusteringKey: includeClusteringKey,
	}
}

func (p *systemColumnPolicy) Split(currentSplit *currentSplit) *currentSplit {
	systemFields := make([]int64, 0, 3)
	systemFieldIndices := make([]int, 0, 3)

	currentSplit.Range(func(idx int, field *schemapb.FieldSchema) {
		if field.GetFieldID() < common.StartOfUserFieldID ||
			(p.includePrimaryKey && field.GetIsPrimaryKey()) ||
			(p.includePartitionKey && field.GetIsPartitionKey()) ||
			(p.includeClusteringKey && field.GetIsClusteringKey()) {
			systemFields = append(systemFields, field.GetFieldID())
			systemFieldIndices = append(systemFieldIndices, idx)
		}
	})

	currentSplit.SplitFields(currentSplit.NextGroupID(), systemFields, systemFieldIndices)
	return currentSplit
}

// remanentShortPolicy merge remanent short fields to a new column group
type remanentShortPolicy struct {
	maxGroupSize int
}

func NewRemanentShortPolicy(maxGroupSize int) ColumnGroupSplitPolicy {
	return &remanentShortPolicy{maxGroupSize: maxGroupSize}
}

func (p *remanentShortPolicy) Split(currentSplit *currentSplit) *currentSplit {
	var shortFields []int64
	var shortFieldIndices []int

	currentSplit.Range(func(idx int, field *schemapb.FieldSchema) {
		shortFields = append(shortFields, field.GetFieldID())
		shortFieldIndices = append(shortFieldIndices, idx)
		if p.maxGroupSize > 0 && len(shortFields) >= p.maxGroupSize {
			currentSplit.SplitFields(currentSplit.NextGroupID(), shortFields, shortFieldIndices)
			shortFields = make([]int64, 0, p.maxGroupSize)
			shortFieldIndices = make([]int, 0, p.maxGroupSize)
		}
	})

	if len(shortFields) > 0 {
		currentSplit.SplitFields(currentSplit.NextGroupID(), shortFields, shortFieldIndices)
	}

	return currentSplit
}

type avgSizePolicy struct {
	sizeThreshold int64
}

func NewAvgSizePolicy(sizeThreshold int64) ColumnGroupSplitPolicy {
	return &avgSizePolicy{sizeThreshold: sizeThreshold}
}

func (p *avgSizePolicy) Split(currentSplit *currentSplit) *currentSplit {
	currentSplit.Range(func(idx int, field *schemapb.FieldSchema) {
		fieldStats, ok := currentSplit.stats[field.GetFieldID()]
		if !ok {
			return
		}
		if fieldStats.AvgSize >= p.sizeThreshold {
			currentSplit.SplitFields(field.GetFieldID(), []int64{field.GetFieldID()}, []int{idx})
		}
	})

	return currentSplit
}
