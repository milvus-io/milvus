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

package job

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const waitCollectionReleasedTimeout = 30 * time.Second

// WaitCollectionReleased blocks until
// all channels and segments of given collection(partitions) are released,
// empty partition list means wait for collection released
func WaitCollectionReleased(ctx context.Context, dist *meta.DistributionManager, checkerController *checkers.CheckerController, collection int64, partitions ...int64) error {
	partitionSet := typeutil.NewUniqueSet(partitions...)
	var (
		lastChannelCount int
		lastSegmentCount int
		lastChangeTime   = time.Now()
	)

	for {
		if err := ctx.Err(); err != nil {
			return errors.Wrapf(err, "context error while waiting for release, collection=%d", collection)
		}

		var (
			channels []*meta.DmChannel
			segments []*meta.Segment = dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(collection))
		)
		if partitionSet.Len() > 0 {
			segments = lo.Filter(segments, func(segment *meta.Segment, _ int) bool {
				return partitionSet.Contain(segment.GetPartitionID())
			})
		} else {
			channels = dist.ChannelDistManager.GetByCollectionAndFilter(collection)
		}

		currentChannelCount := len(channels)
		currentSegmentCount := len(segments)
		if currentChannelCount+currentSegmentCount == 0 {
			break
		}

		// If release is in progress, reset last change time
		if currentChannelCount < lastChannelCount || currentSegmentCount < lastSegmentCount {
			lastChangeTime = time.Now()
		}

		// If release is not in progress for a while, return error
		if time.Since(lastChangeTime) > waitCollectionReleasedTimeout {
			return errors.Errorf("wait collection released timeout, collection=%d, channels=%d, segments=%d",
				collection, currentChannelCount, currentSegmentCount)
		}

		log.Ctx(ctx).Info("waitting for release...",
			zap.Int64("collection", collection),
			zap.Int64s("partitions", partitions),
			zap.Int("channel", currentChannelCount),
			zap.Int("segments", currentSegmentCount),
		)

		lastChannelCount = currentChannelCount
		lastSegmentCount = currentSegmentCount

		// trigger check more frequently
		checkerController.Check()
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}

func WaitCurrentTargetUpdated(ctx context.Context, targetObserver *observers.TargetObserver, collection int64) error {
	// manual trigger update next target
	ready, err := targetObserver.UpdateNextTarget(collection)
	if err != nil {
		return errors.Wrapf(err, "failed to update next target, collection=%d", collection)
	}

	// accelerate check
	targetObserver.TriggerUpdateCurrentTarget(collection)
	// wait current target ready
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return errors.Wrapf(ctx.Err(), "context error while waiting for current target updated, collection=%d", collection)
	case <-time.After(waitCollectionReleasedTimeout):
		return errors.Errorf("wait current target updated timeout, collection=%d", collection)
	}
}
