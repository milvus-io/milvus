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
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type ColumnGroup struct {
	Columns []int // column indices
}

// split by row average size
func SplitByFieldSize(fieldBinlogs []*datapb.FieldBinlog, splitThresHold int64) []ColumnGroup {
	groups := make([]ColumnGroup, 0)
	shortColumnGroup := ColumnGroup{Columns: make([]int, 0)}
	for i, fieldBinlog := range fieldBinlogs {
		if len(fieldBinlog.Binlogs) == 0 {
			continue
		}
		totalSize := lo.SumBy(fieldBinlog.Binlogs, func(b *datapb.Binlog) int64 { return b.LogSize })
		totalNumRows := lo.SumBy(fieldBinlog.Binlogs, func(b *datapb.Binlog) int64 { return b.EntriesNum })
		if totalSize/totalNumRows >= splitThresHold {
			groups = append(groups, ColumnGroup{Columns: []int{i}})
		} else {
			shortColumnGroup.Columns = append(shortColumnGroup.Columns, i)
		}
	}
	if len(shortColumnGroup.Columns) > 0 {
		groups = append(groups, shortColumnGroup)
	}
	return groups
}
