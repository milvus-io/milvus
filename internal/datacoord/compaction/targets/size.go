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

package targets

import (
	"context"
	"errors"
	"math"
	"strconv"

	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type SizeCondition struct {
	Enabled      bool
	MaxSize      int64
	DiskMaxSize  int64
	MaxFragments int64
}

func ReadSizeCondition() (SizeCondition, error) {
	return readSizeCondition(paramtable.Get().DataCoordCfg.EnableAutoCompaction.GetAsBool())
}

func readSizeCondition(auto bool) (SizeCondition, error) {
	cfg := &paramtable.Get().DataCoordCfg
	condition := SizeCondition{Enabled: auto, MaxFragments: -1}
	var invalid []string
	for _, setting := range []struct {
		key, value string
		output     *int64
	}{
		{cfg.SegmentMaxSize.Key, cfg.SegmentMaxSize.GetValue(), &condition.MaxSize},
		{cfg.DiskSegmentMaxSize.Key, cfg.DiskSegmentMaxSize.GetValue(), &condition.DiskMaxSize},
	} {
		const mib = 1024 * 1024
		value, err := strconv.ParseInt(setting.value, 10, 64)
		if err != nil || value <= 0 || value > math.MaxInt64/mib {
			invalid = append(invalid, setting.key)
			continue
		}
		*setting.output = value * mib
	}
	fragments, err := strconv.ParseInt(cfg.MaxFragmentsPerGroup.GetValue(), 10, 64)
	if err == nil && fragments >= 0 {
		condition.MaxFragments = fragments
	} else {
		invalid = append(invalid, cfg.MaxFragmentsPerGroup.Key)
	}
	if len(invalid) > 0 {
		return condition, merr.WrapErrServiceUnavailableMsg("invalid SIZE configuration: %v", invalid)
	}
	return condition, nil
}

func (condition SizeCondition) Parameters() map[string]string {
	parameters := map[string]string{"planner": "two_tier"}
	if condition.MaxSize > 0 {
		parameters["max_size_bytes"] = strconv.FormatInt(condition.MaxSize, 10)
	}
	if condition.DiskMaxSize > 0 {
		parameters["disk_max_size_bytes"] = strconv.FormatInt(condition.DiskMaxSize, 10)
	}
	if condition.MaxFragments >= 0 {
		parameters["max_fragments_per_group"] = strconv.FormatInt(condition.MaxFragments, 10)
	}
	return parameters
}

func (condition SizeCondition) expected(indexes *index.IndexMeta, collection *meta.CollectionInfo) int64 {
	if policy.UsesDiskSegmentSize(indexes, collection.ID, collection.Schema) {
		return condition.DiskMaxSize
	}
	return condition.MaxSize
}

type sizeInput struct {
	segment  *meta.SegmentInfo
	residual int64
	expected int64
}

// ObserveSize evaluates the complete semantic Match independently of import,
// execution readiness, placement and overlapping OPTIMIZE scopes.
func ObserveSize(ctx context.Context, mt *meta.Meta, handler meta.Handler, indexes *index.IndexMeta, condition SizeCondition) (int64, error) {
	var rows int64
	err := visitSizeMatches(ctx, mt, handler, indexes, condition, standingSegments(ctx, mt), func(input sizeInput) error {
		count := input.segment.GetNumOfRows()
		if count > math.MaxInt64-rows {
			return merr.WrapErrDataIntegrityMsg("overflowing SIZE Match row count at segment %d", input.segment.GetID())
		}
		rows += count
		return nil
	})
	return rows, err
}

func visitSizeMatches(
	ctx context.Context, mt *meta.Meta, handler meta.Handler, indexes *index.IndexMeta, condition SizeCondition,
	segments []*meta.SegmentInfo, match func(sizeInput) error,
) error {
	byCollection := make(map[int64]int64)
	var firstError error
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if segment.GetLevel() == datapb.SegmentLevel_L2 {
			continue
		}
		expected, observed := byCollection[segment.GetCollectionID()]
		if !observed {
			collection := mt.GetCollection(segment.GetCollectionID())
			if collection == nil {
				var err error
				collection, err = handler.GetCollection(ctx, segment.GetCollectionID())
				if err != nil && !errors.Is(err, merr.ErrCollectionNotFound) && firstError == nil {
					firstError = merr.Wrapf(err, "observe SIZE collection %d", segment.GetCollectionID())
				}
			}
			if collection != nil && !collection.IsExternal() {
				expected = condition.expected(indexes, collection)
			}
			byCollection[segment.GetCollectionID()] = expected
		}
		if expected <= 0 {
			continue
		}
		size := segment.GetResidualSegmentSize()
		if segment.GetNumOfRows() < 0 || size < 0 {
			if firstError == nil {
				firstError = merr.WrapErrDataIntegrityMsg("invalid SIZE observation at segment %d", segment.GetID())
			}
			continue
		}
		if size < view.TwoTierFullThreshold(expected) {
			if err := match(sizeInput{segment: segment, residual: size, expected: expected}); err != nil {
				return err
			}
		}
	}
	return firstError
}
