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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	pendingGroups []localFormatGroup
}

func newCurrentSplit(fields []*schemapb.FieldSchema, stats map[int64]ColumnStats) *currentSplit {
	pendingGroup := localFormatGroup{
		fields:      make([]int64, 0, len(fields)),
		indices:     make([]int, 0, len(fields)),
		localFormat: localFormatDefault,
	}
	for idx, field := range fields {
		pendingGroup.fields = append(pendingGroup.fields, field.GetFieldID())
		pendingGroup.indices = append(pendingGroup.indices, idx)
	}
	return &currentSplit{
		fields:        fields,
		stats:         stats,
		processFields: typeutil.NewSet[int64](),
		pendingGroups: []localFormatGroup{pendingGroup},
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
	for _, group := range c.RangeGroups(nil) {
		for _, idx := range group.indices {
			f(idx, c.fields[idx])
		}
	}
}

func (c *currentSplit) RangeGroups(match func(*schemapb.FieldSchema) bool) []localFormatGroup {
	pendingGroups := c.pendingGroups
	if len(pendingGroups) == 0 {
		pendingGroups = []localFormatGroup{{}}
		for idx, field := range c.fields {
			pendingGroups[0].fields = append(pendingGroups[0].fields, field.GetFieldID())
			pendingGroups[0].indices = append(pendingGroups[0].indices, idx)
		}
	}

	groups := make([]localFormatGroup, 0, len(pendingGroups))
	for _, pendingGroup := range pendingGroups {
		group := localFormatGroup{
			fields:      make([]int64, 0, len(pendingGroup.fields)),
			indices:     make([]int, 0, len(pendingGroup.indices)),
			localFormat: pendingGroup.localFormat,
		}
		for _, idx := range pendingGroup.indices {
			field := c.fields[idx]
			if c.Processed(field.GetFieldID()) {
				continue
			}
			if match != nil && !match(field) {
				continue
			}
			group.fields = append(group.fields, field.GetFieldID())
			group.indices = append(group.indices, idx)
		}
		if len(group.fields) > 0 {
			groups = append(groups, group)
		}
	}
	return groups
}

func (c *currentSplit) PartitionRemainingByLocalFormat() {
	nextGroups := make([]localFormatGroup, 0, len(c.pendingGroups))
	for _, pendingGroup := range c.RangeGroups(nil) {
		groupsByFormat := make(map[string]*localFormatGroup)
		formats := make([]string, 0, 3)
		for _, idx := range pendingGroup.indices {
			field := c.fields[idx]
			format := fieldLocalFormat(field)
			group := groupsByFormat[format]
			if group == nil {
				formats = append(formats, format)
				group = &localFormatGroup{localFormat: format}
				groupsByFormat[format] = group
			}
			group.fields = append(group.fields, field.GetFieldID())
			group.indices = append(group.indices, idx)
		}
		for _, format := range formats {
			nextGroups = append(nextGroups, *groupsByFormat[format])
		}
	}
	c.pendingGroups = nextGroups
}

// ColumnGroupSplitPolicy interface for column group split policy.
type ColumnGroupSplitPolicy interface {
	Split(currentSplit *currentSplit) *currentSplit
}

// selectedDataTypePolicy splits wide data types (vector, text) to new column groups.
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

// localFormatPolicy only partitions fields by local loading intent. It must not
// set ColumnGroup.Format, which is physical writer metadata owned by the writer
// configuration and existing manifests.
type localFormatPolicy struct{}

const localFormatDefault = ""

type localFormatGroup struct {
	fields      []int64
	indices     []int
	localFormat string
}

func fieldLocalFormat(field *schemapb.FieldSchema) string {
	for _, kv := range field.GetTypeParams() {
		if kv.GetKey() == common.LocalFormatKey {
			return kv.GetValue()
		}
	}
	// Keep the default value distinct from explicit raw so a server-level default
	// can be introduced without merging fields with different local intent.
	return localFormatDefault
}

func (p *localFormatPolicy) Split(currentSplit *currentSplit) *currentSplit {
	currentSplit.PartitionRemainingByLocalFormat()
	return currentSplit
}

func NewLocalFormatPolicy() ColumnGroupSplitPolicy {
	return &localFormatPolicy{}
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
	groups := currentSplit.RangeGroups(func(field *schemapb.FieldSchema) bool {
		return field.GetFieldID() < common.StartOfUserFieldID ||
			(p.includePrimaryKey && field.GetIsPrimaryKey()) ||
			(p.includePartitionKey && field.GetIsPartitionKey()) ||
			(p.includeClusteringKey && field.GetIsClusteringKey())
	})

	for _, group := range groups {
		currentSplit.SplitFields(currentSplit.NextGroupID(), group.fields, group.indices)
	}
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
	for _, group := range currentSplit.RangeGroups(nil) {
		shortFields := make([]int64, 0, len(group.fields))
		shortFieldIndices := make([]int, 0, len(group.indices))
		for i, fieldID := range group.fields {
			shortFields = append(shortFields, fieldID)
			shortFieldIndices = append(shortFieldIndices, group.indices[i])
			if p.maxGroupSize > 0 && len(shortFields) >= p.maxGroupSize {
				currentSplit.SplitFields(currentSplit.NextGroupID(), shortFields, shortFieldIndices)
				shortFields = make([]int64, 0, p.maxGroupSize)
				shortFieldIndices = make([]int, 0, p.maxGroupSize)
			}
		}

		if len(shortFields) > 0 {
			currentSplit.SplitFields(currentSplit.NextGroupID(), shortFields, shortFieldIndices)
		}
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
