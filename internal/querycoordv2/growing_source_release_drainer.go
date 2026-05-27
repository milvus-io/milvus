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

package querycoordv2

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ task.GrowingSourceReleaseDrainer = (*growingSourceReleaseDrainer)(nil)

type broadcastStarter func(ctx context.Context, collectionID int64) (broadcaster.BroadcastAPI, error)

type growingFlushProgressGetter interface {
	GetGrowingFlushProgress(ctx context.Context, vchannel string, segmentIDs []int64, fenceTs uint64) ([]writebuffer.GrowingFlushSegmentProgress, error)
}

type growingFlushProgressGetterFunc func(ctx context.Context, vchannel string, segmentIDs []int64, fenceTs uint64) ([]writebuffer.GrowingFlushSegmentProgress, error)

func (f growingFlushProgressGetterFunc) GetGrowingFlushProgress(ctx context.Context, vchannel string, segmentIDs []int64, fenceTs uint64) ([]writebuffer.GrowingFlushSegmentProgress, error) {
	return f(ctx, vchannel, segmentIDs, fenceTs)
}

// growingSourceReleaseDrainer runs WAL fence + same-process WAL flusher handoff before growing-source sources are released.
type growingSourceReleaseDrainer struct {
	broker                     meta.Broker
	targetMgr                  meta.TargetManagerInterface
	startBroadcast             broadcastStarter
	growingFlushProgressGetter growingFlushProgressGetter
}

func newGrowingSourceReleaseDrainer(
	broker meta.Broker,
	targetMgr meta.TargetManagerInterface,
	startBroadcast broadcastStarter,
	growingFlushProgressGetter growingFlushProgressGetter,
) *growingSourceReleaseDrainer {
	return &growingSourceReleaseDrainer{
		broker:                     broker,
		targetMgr:                  targetMgr,
		startBroadcast:             startBroadcast,
		growingFlushProgressGetter: growingFlushProgressGetter,
	}
}

func (d *growingSourceReleaseDrainer) DrainGrowingSourceReleaseChannels(ctx context.Context, collectionID int64, channels []string) (map[string]uint64, error) {
	coll, err := d.broker.DescribeCollection(ctx, collectionID)
	if err != nil {
		return nil, err
	}

	broadcastAPI, err := d.startBroadcast(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	defer broadcastAPI.Close()

	return d.DrainChannels(ctx, broadcastAPI, coll, channels)
}

func (d *growingSourceReleaseDrainer) DrainGrowingSourceReleaseSegments(ctx context.Context, collectionID int64, segmentsByChannel map[string][]int64) (map[string]uint64, error) {
	segmentsByChannel = normalizeGrowingSourceReleaseSegments(segmentsByChannel)
	if len(segmentsByChannel) == 0 {
		return nil, nil
	}

	coll, err := d.broker.DescribeCollection(ctx, collectionID)
	if err != nil {
		return nil, err
	}

	channels := make([]string, 0, len(segmentsByChannel))
	for channel := range segmentsByChannel {
		channels = append(channels, channel)
	}

	broadcastAPI, err := d.startBroadcast(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	defer broadcastAPI.Close()

	return d.drainChannels(ctx, broadcastAPI, coll, channels, segmentsByChannel)
}

func (d *growingSourceReleaseDrainer) DrainChannels(
	ctx context.Context,
	broadcastAPI broadcaster.BroadcastAPI,
	coll *milvuspb.DescribeCollectionResponse,
	vchannels []string,
) (map[string]uint64, error) {
	return d.drainChannels(ctx, broadcastAPI, coll, vchannels, nil)
}

func (d *growingSourceReleaseDrainer) drainChannels(
	ctx context.Context,
	broadcastAPI broadcaster.BroadcastAPI,
	coll *milvuspb.DescribeCollectionResponse,
	vchannels []string,
	releaseSegmentsByChannel map[string][]int64,
) (map[string]uint64, error) {
	if !shouldUseGrowingSourceFlush(coll) {
		return nil, nil
	}

	vchannels = lo.Uniq(lo.Filter(vchannels, func(channel string, _ int) bool {
		return channel != ""
	}))
	if releaseSegmentsByChannel != nil {
		releaseSegmentsByChannel = normalizeGrowingSourceReleaseSegments(releaseSegmentsByChannel)
		vchannels = lo.Filter(vchannels, func(channel string, _ int) bool {
			return len(releaseSegmentsByChannel[channel]) > 0
		})
	}
	sort.Strings(vchannels)
	if len(vchannels) == 0 {
		return nil, nil
	}

	log.Ctx(ctx).Info("append release fence for growing-source collection",
		zap.Int64("collectionID", coll.GetCollectionID()),
		zap.Strings("channels", vchannels))

	growingSourceReleaseFenceKey, growingSourceReleaseFenceValue := message.GrowingSourceReleaseFenceProperty()
	msg := message.NewManualFlushMessageBuilderV2().
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: coll.GetCollectionID(),
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		WithProperty(growingSourceReleaseFenceKey, growingSourceReleaseFenceValue).
		WithBroadcast(vchannels).
		MustBuildBroadcast()
	result, err := broadcastAPI.Broadcast(ctx, msg)
	if err != nil {
		return nil, err
	}

	fenceTs := make(map[string]uint64, len(vchannels))
	fencedSegments := make(map[string][]int64, len(vchannels))
	for _, channel := range vchannels {
		appendResult := result.GetAppendResult(channel)
		if appendResult == nil || appendResult.TimeTick == 0 {
			return nil, errors.Errorf("release fence append result missing for channel %s", channel)
		}
		fenceTs[channel] = appendResult.TimeTick
		if appendResult.Extra != nil {
			var extra message.ManualFlushExtraResponse
			if err := appendResult.GetExtra(&extra); err != nil {
				return nil, errors.Wrapf(err, "failed to get release fence segment ids for channel %s", channel)
			}
			fencedSegments[channel] = extra.GetSegmentIds()
		}
	}

	log.Ctx(ctx).Info("wait release handoff prepared for growing-source collection",
		zap.Int64("collectionID", coll.GetCollectionID()),
		zap.Any("fenceTs", fenceTs),
		zap.Any("fencedSegments", fencedSegments),
		zap.Any("prepareSegments", fencedSegments))
	waitCtx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.GrowingSourceReleaseDrainTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	if err := d.prepareGrowingSourceReleaseHandoff(waitCtx, coll.GetCollectionID(), vchannels, fenceTs, fencedSegments); err != nil {
		return nil, err
	}

	log.Ctx(ctx).Info("release handoff prepared for growing-source collection",
		zap.Int64("collectionID", coll.GetCollectionID()),
		zap.Any("fenceTs", fenceTs))
	return fenceTs, nil
}

func normalizeGrowingSourceReleaseSegments(segmentsByChannel map[string][]int64) map[string][]int64 {
	normalized := make(map[string][]int64, len(segmentsByChannel))
	for channel, segmentIDs := range segmentsByChannel {
		if channel == "" {
			continue
		}
		segmentIDs = lo.Uniq(segmentIDs)
		if len(segmentIDs) == 0 {
			continue
		}
		normalized[channel] = segmentIDs
	}
	return normalized
}

func (d *growingSourceReleaseDrainer) prepareGrowingSourceReleaseHandoff(
	ctx context.Context,
	collectionID int64,
	vchannels []string,
	fenceTs map[string]uint64,
	fencedSegments map[string][]int64,
) error {
	for _, channel := range vchannels {
		if err := d.prepareHandoffOnFlusher(ctx, channel, fencedSegments[channel], fenceTs[channel]); err != nil {
			return errors.Wrapf(err, "prepare growing-source release handoff on WAL flusher for channel %s collection %d", channel, collectionID)
		}
	}
	return nil
}

func (d *growingSourceReleaseDrainer) prepareHandoffOnFlusher(ctx context.Context, channel string, segmentIDs []int64, fenceTs uint64) error {
	if d.growingFlushProgressGetter == nil {
		return errors.New("growing flush progress getter is not initialized")
	}
	_, err := d.growingFlushProgressGetter.GetGrowingFlushProgress(ctx, channel, segmentIDs, fenceTs)
	return err
}

func (d *growingSourceReleaseDrainer) ReleaseDrainChannels(ctx context.Context, collectionID int64) []string {
	channelSet := make(map[string]struct{})
	for _, scope := range []meta.TargetScope{meta.CurrentTarget, meta.NextTarget} {
		for channel := range d.targetMgr.GetDmChannelsByCollection(ctx, collectionID, scope) {
			channelSet[channel] = struct{}{}
		}
	}

	channels := make([]string, 0, len(channelSet))
	for channel := range channelSet {
		channels = append(channels, channel)
	}
	return channels
}
