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

package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type CompactionTriggerType int8

const (
	TriggerTypeLevelZeroViewChange CompactionTriggerType = iota + 1
	TriggerTypeLevelZeroViewIDLE
	TriggerTypeLevelZeroViewManual
	TriggerTypeSegmentSizeViewChange
	TriggerTypeClustering
	TriggerTypeSingle
	TriggerTypeSort
	TriggerTypeForceMerge
	TriggerTypeStorageVersionUpgrade
	TriggerTypeBumpSchemaVersion
)

func (t CompactionTriggerType) GetCompactionType() datapb.CompactionType {
	switch t {
	case TriggerTypeLevelZeroViewChange, TriggerTypeLevelZeroViewIDLE, TriggerTypeLevelZeroViewManual:
		return datapb.CompactionType_Level0DeleteCompaction
	case TriggerTypeSegmentSizeViewChange, TriggerTypeSingle, TriggerTypeForceMerge:
		return datapb.CompactionType_MixCompaction
	case TriggerTypeClustering:
		return datapb.CompactionType_ClusteringCompaction
	case TriggerTypeSort:
		return datapb.CompactionType_SortCompaction
	case TriggerTypeStorageVersionUpgrade:
		return datapb.CompactionType_MixCompaction
	case TriggerTypeBumpSchemaVersion:
		return datapb.CompactionType_BumpSchemaVersionCompaction
	default:
		return datapb.CompactionType_MixCompaction
	}
}

func (t CompactionTriggerType) String() string {
	switch t {
	case TriggerTypeLevelZeroViewChange:
		return "LevelZeroViewChange"
	case TriggerTypeLevelZeroViewIDLE:
		return "LevelZeroViewIDLE"
	case TriggerTypeLevelZeroViewManual:
		return "LevelZeroViewManual"
	case TriggerTypeSegmentSizeViewChange:
		return "SegmentSizeViewChange"
	case TriggerTypeClustering:
		return "Clustering"
	case TriggerTypeSingle:
		return "Single"
	case TriggerTypeSort:
		return "Sort"
	case TriggerTypeForceMerge:
		return "ForceMerge"
	case TriggerTypeStorageVersionUpgrade:
		return "StorageVersionUpgrade"
	case TriggerTypeBumpSchemaVersion:
		return "BumpSchemaVersion"
	default:
		return ""
	}
}

// CompactionPolicy defines the interface for different compaction policies
type CompactionPolicy interface {
	// Enable returns whether this compaction policy is enabled
	Enable() bool
	// Trigger returns views that require inspector slots (actual compaction tasks).
	// Only called when the inspector is not full.
	Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error)
	// Name returns the name of this compaction policy
	Name() string
}
