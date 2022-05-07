package querynode

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

// queryChannel simple query channel wrapper in query shard service
type queryChannel struct {
	closeCh      chan struct{}
	collectionID int64

	streaming      *streaming
	queryMsgStream msgstream.MsgStream
	shardCluster   *ShardClusterService
	asConsumeOnce  sync.Once
	closeOnce      sync.Once
}

// NewQueryChannel create a query channel with provided shardCluster, query msgstream and collection id
func NewQueryChannel(collectionID int64, scs *ShardClusterService, qms msgstream.MsgStream, streaming *streaming) *queryChannel {
	return &queryChannel{
		closeCh:      make(chan struct{}),
		collectionID: collectionID,

		streaming:      streaming,
		queryMsgStream: qms,
		shardCluster:   scs,
	}
}

// AsConsumer do AsConsumer for query msgstream and seek if position is not nil
func (qc *queryChannel) AsConsumer(channelName string, subName string, position *internalpb.MsgPosition) error {
	var err error
	qc.asConsumeOnce.Do(func() {
		qc.queryMsgStream.AsConsumer([]string{channelName}, subName)
		metrics.QueryNodeNumConsumers.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Inc()
		if position == nil || len(position.MsgID) == 0 {
			log.Info("QueryNode AsConsumer", zap.String("channel", channelName), zap.String("sub name", subName))
		} else {
			err = qc.queryMsgStream.Seek([]*internalpb.MsgPosition{position})
			if err == nil {
				log.Info("querynode seek query channel: ", zap.Any("consumeChannel", channelName),
					zap.String("seek position", string(position.MsgID)))
			}
		}
	})

	return err
}

// Start start a goroutine for consume msg
func (qc *queryChannel) Start() {
	go qc.queryMsgStream.Start()

	go qc.consumeQuery()
}

// Stop all workers and msgstream
func (qc *queryChannel) Stop() {
	qc.closeOnce.Do(func() {
		qc.queryMsgStream.Close()
		close(qc.closeCh)
	})
}

func (qc *queryChannel) consumeQuery() {
	for {
		select {
		case <-qc.closeCh:
			log.Info("query channel worker quit", zap.Int64("collection id", qc.collectionID))
			return
		case msgPack, ok := <-qc.queryMsgStream.Chan():
			if !ok {
				log.Warn("Receive Query Msg from chan failed", zap.Int64("collectionID", qc.collectionID))
				return
			}
			if !ok || msgPack == nil || len(msgPack.Msgs) == 0 {
				continue
			}

			for _, msg := range msgPack.Msgs {
				switch sm := msg.(type) {
				case *msgstream.SealedSegmentsChangeInfoMsg:
					qc.adjustByChangeInfo(sm)
				default:
					log.Warn("ignore msgs other than SegmentChangeInfo", zap.Any("msgType", msg.Type().String()))
				}
			}
		}
	}
}

func (qc *queryChannel) adjustByChangeInfo(msg *msgstream.SealedSegmentsChangeInfoMsg) {
	for _, info := range msg.Infos {
		// precheck collection id, if not the same collection, skip
		for _, segment := range info.OnlineSegments {
			if segment.CollectionID != qc.collectionID {
				return
			}
		}

		for _, segment := range info.OfflineSegments {
			if segment.CollectionID != qc.collectionID {
				return
			}
		}

		// process change in shard cluster
		qc.shardCluster.HandoffSegments(qc.collectionID, info)

		// for OnlineSegments:
		for _, segment := range info.OnlineSegments {
			/*
				// 1. update global sealed segments
				q.globalSegmentManager.addGlobalSegmentInfo(segment)
				// 2. update excluded segment, cluster have been loaded sealed segments,
				// so we need to avoid getting growing segment from flow graph.*/
			qc.streaming.replica.addExcludedSegments(segment.CollectionID, []*datapb.SegmentInfo{
				{
					ID:            segment.SegmentID,
					CollectionID:  segment.CollectionID,
					PartitionID:   segment.PartitionID,
					InsertChannel: segment.DmChannel,
					NumOfRows:     segment.NumRows,
					// TODO: add status, remove query pb segment status, use common pb segment status?
					DmlPosition: &internalpb.MsgPosition{
						// use max timestamp to filter out dm messages
						Timestamp: typeutil.MaxTimestamp,
					},
				},
			})
		}

		log.Info("Successfully changed global sealed segment info ",
			zap.Int64("collection ", qc.collectionID),
			zap.Any("online segments ", info.OnlineSegments),
			zap.Any("offline segments ", info.OfflineSegments))
	}
}
