package datanode

import (
	"fmt"
	"sync"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type statsUpdater interface {
	update(channel string, ts Timestamp, stats []*commonpb.SegmentStats)
}

// mqStatsUpdater is the wrapper of mergedTimeTickSender
type mqStatsUpdater struct {
	sender   *mergedTimeTickerSender
	producer msgstream.MsgStream
	config   *nodeConfig

	mut   sync.Mutex
	stats map[int64]int64 // segment id => row nums
}

func newMqStatsUpdater(config *nodeConfig, producer msgstream.MsgStream) statsUpdater {
	updater := &mqStatsUpdater{
		stats:    make(map[int64]int64),
		producer: producer,
		config:   config,
	}
	sender := newUniqueMergedTimeTickerSender(updater.send)
	updater.sender = sender
	return updater
}

func (u *mqStatsUpdater) send(ts Timestamp, segmentIDs []int64) error {
	u.mut.Lock()
	defer u.mut.Unlock()
	stats := lo.Map(segmentIDs, func(id int64, _ int) *commonpb.SegmentStats {
		rowNum := u.stats[id]
		return &commonpb.SegmentStats{
			SegmentID: id,
			NumRows:   rowNum,
		}
	})

	msgPack := msgstream.MsgPack{}
	timeTickMsg := msgstream.DataNodeTtMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		DataNodeTtMsg: &msgpb.DataNodeTtMsg{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_DataNodeTt),
				commonpbutil.WithTimeStamp(ts),
				commonpbutil.WithSourceID(u.config.serverID),
			),
			ChannelName:   u.config.vChannelName,
			Timestamp:     ts,
			SegmentsStats: stats,
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, &timeTickMsg)
	sub := tsoutil.SubByNow(ts)
	pChan := funcutil.ToPhysicalChannel(u.config.vChannelName)
	metrics.DataNodeProduceTimeTickLag.
		WithLabelValues(fmt.Sprint(u.config.serverID), pChan).
		Set(float64(sub))
	err := u.producer.Produce(&msgPack)
	if err != nil {
		return err
	}

	for _, segmentID := range segmentIDs {
		delete(u.stats, segmentID)
	}
	return nil
}

func (u *mqStatsUpdater) update(channel string, ts Timestamp, stats []*commonpb.SegmentStats) {
	u.mut.Lock()
	defer u.mut.Unlock()
	segmentIDs := lo.Map(stats, func(stats *commonpb.SegmentStats, _ int) int64 { return stats.SegmentID })

	lo.ForEach(stats, func(stats *commonpb.SegmentStats, _ int) {
		u.stats[stats.SegmentID] = stats.NumRows
	})

	u.sender.bufferTs(ts, segmentIDs)
}
