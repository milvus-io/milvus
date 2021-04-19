package querynode

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	queryPb "github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type task interface {
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	Timestamp() Timestamp
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
	OnEnqueue() error
}

type baseTask struct {
	done chan error
	ctx  context.Context
	id   UniqueID
}

type watchDmChannelsTask struct {
	baseTask
	req  *queryPb.WatchDmChannelsRequest
	node *QueryNode
}

type loadSegmentsTask struct {
	baseTask
	req  *queryPb.LoadSegmentsRequest
	node *QueryNode
}

type releaseCollectionTask struct {
	baseTask
	req  *queryPb.ReleaseCollectionRequest
	node *QueryNode
}

type releasePartitionsTask struct {
	baseTask
	req  *queryPb.ReleasePartitionsRequest
	node *QueryNode
}

func (b *baseTask) ID() UniqueID {
	return b.id
}

func (b *baseTask) SetID(uid UniqueID) {
	b.id = uid
}

func (b *baseTask) WaitToFinish() error {
	err := <-b.done
	return err
}

func (b *baseTask) Notify(err error) {
	b.done <- err
}

// watchDmChannelsTask
func (w *watchDmChannelsTask) Timestamp() Timestamp {
	if w.req.Base == nil {
		log.Error("nil base req in watchDmChannelsTask", zap.Any("collectionID", w.req.CollectionID))
		return 0
	}
	return w.req.Base.Timestamp
}

func (w *watchDmChannelsTask) OnEnqueue() error {
	if w.req == nil || w.req.Base == nil {
		w.SetID(rand.Int63n(100000000000))
	} else {
		w.SetID(w.req.Base.MsgID)
	}
	return nil
}

func (w *watchDmChannelsTask) PreExecute(ctx context.Context) error {
	return nil
}

func (w *watchDmChannelsTask) Execute(ctx context.Context) error {
	log.Debug("starting WatchDmChannels ...", zap.String("ChannelIDs", fmt.Sprintln(w.req.ChannelIDs)))
	collectionID := w.req.CollectionID
	ds, err := w.node.getDataSyncService(collectionID)
	if err != nil || ds.dmStream == nil {
		errMsg := "null data sync service or null data manipulation stream, collectionID = " + fmt.Sprintln(collectionID)
		log.Error(errMsg)
		return errors.New(errMsg)
	}

	switch t := ds.dmStream.(type) {
	case *msgstream.MqTtMsgStream:
	default:
		_ = t
		errMsg := "type assertion failed for dm message stream"
		log.Error(errMsg)
		return errors.New(errMsg)
	}

	getUniqueSubName := func() string {
		prefixName := Params.MsgChannelSubName
		return prefixName + "-" + strconv.FormatInt(collectionID, 10)
	}

	// add request channel
	consumeChannels := w.req.ChannelIDs
	toSeekInfo := make([]*internalpb.MsgPosition, 0)
	toDirSubChannels := make([]string, 0)

	consumeSubName := getUniqueSubName()

	for _, info := range w.req.Infos {
		if len(info.Pos.MsgID) == 0 {
			toDirSubChannels = append(toDirSubChannels, info.ChannelID)
			continue
		}
		info.Pos.MsgGroup = consumeSubName
		toSeekInfo = append(toSeekInfo, info.Pos)

		log.Debug("prevent inserting segments", zap.String("segmentIDs", fmt.Sprintln(info.ExcludedSegments)))
		err := w.node.replica.addExcludedSegments(collectionID, info.ExcludedSegments)
		if err != nil {
			log.Error(err.Error())
			return err
		}
	}

	ds.dmStream.AsConsumer(toDirSubChannels, consumeSubName)
	for _, pos := range toSeekInfo {
		err := ds.dmStream.Seek(pos)
		if err != nil {
			errMsg := "msgStream seek error :" + err.Error()
			log.Error(errMsg)
			return errors.New(errMsg)
		}
	}
	log.Debug("querynode AsConsumer: " + strings.Join(consumeChannels, ", ") + " : " + consumeSubName)
	log.Debug("WatchDmChannels done", zap.String("ChannelIDs", fmt.Sprintln(w.req.ChannelIDs)))
	return nil
}

func (w *watchDmChannelsTask) PostExecute(ctx context.Context) error {
	return nil
}

// loadSegmentsTask
func (l *loadSegmentsTask) Timestamp() Timestamp {
	if l.req.Base == nil {
		log.Error("nil base req in loadSegmentsTask", zap.Any("collectionID", l.req.CollectionID))
		return 0
	}
	return l.req.Base.Timestamp
}

func (l *loadSegmentsTask) OnEnqueue() error {
	if l.req == nil || l.req.Base == nil {
		l.SetID(rand.Int63n(100000000000))
	} else {
		l.SetID(l.req.Base.MsgID)
	}
	return nil
}

func (l *loadSegmentsTask) PreExecute(ctx context.Context) error {
	return nil
}

func (l *loadSegmentsTask) Execute(ctx context.Context) error {
	// TODO: support db
	collectionID := l.req.CollectionID
	partitionID := l.req.PartitionID
	segmentIDs := l.req.SegmentIDs
	fieldIDs := l.req.FieldIDs
	schema := l.req.Schema

	log.Debug("query node load segment", zap.String("loadSegmentRequest", fmt.Sprintln(l.req)))

	hasCollection := l.node.replica.hasCollection(collectionID)
	hasPartition := l.node.replica.hasPartition(partitionID)
	if !hasCollection {
		// loading init
		err := l.node.replica.addCollection(collectionID, schema)
		if err != nil {
			return err
		}
		l.node.replica.initExcludedSegments(collectionID)
		newDS := newDataSyncService(l.node.queryNodeLoopCtx, l.node.replica, l.node.msFactory, collectionID)
		// ignore duplicated dataSyncService error
		_ = l.node.addDataSyncService(collectionID, newDS)
		ds, err := l.node.getDataSyncService(collectionID)
		if err != nil {
			return err
		}
		go ds.start()
		l.node.searchService.startSearchCollection(collectionID)
	}
	if !hasPartition {
		err := l.node.replica.addPartition(collectionID, partitionID)
		if err != nil {
			return err
		}
	}
	err := l.node.replica.enablePartition(partitionID)
	if err != nil {
		return err
	}

	if len(segmentIDs) == 0 {
		return nil
	}

	err = l.node.loadService.loadSegmentPassively(collectionID, partitionID, segmentIDs, fieldIDs)
	if err != nil {
		return err
	}

	log.Debug("LoadSegments done", zap.String("segmentIDs", fmt.Sprintln(l.req.SegmentIDs)))
	return nil
}

func (l *loadSegmentsTask) PostExecute(ctx context.Context) error {
	return nil
}

// releaseCollectionTask
func (r *releaseCollectionTask) Timestamp() Timestamp {
	if r.req.Base == nil {
		log.Error("nil base req in releaseCollectionTask", zap.Any("collectionID", r.req.CollectionID))
		return 0
	}
	return r.req.Base.Timestamp
}

func (r *releaseCollectionTask) OnEnqueue() error {
	if r.req == nil || r.req.Base == nil {
		r.SetID(rand.Int63n(100000000000))
	} else {
		r.SetID(r.req.Base.MsgID)
	}
	return nil
}

func (r *releaseCollectionTask) PreExecute(ctx context.Context) error {
	return nil
}

func (r *releaseCollectionTask) Execute(ctx context.Context) error {
	ds, err := r.node.getDataSyncService(r.req.CollectionID)
	if err == nil && ds != nil {
		ds.close()
		r.node.removeDataSyncService(r.req.CollectionID)
		r.node.replica.removeTSafe(r.req.CollectionID)
		r.node.replica.removeExcludedSegments(r.req.CollectionID)
	}

	if r.node.searchService.hasSearchCollection(r.req.CollectionID) {
		r.node.searchService.stopSearchCollection(r.req.CollectionID)
	}

	err = r.node.replica.removeCollection(r.req.CollectionID)
	if err != nil {
		return err
	}

	log.Debug("ReleaseCollection done", zap.Int64("collectionID", r.req.CollectionID))
	return nil
}

func (r *releaseCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

// releasePartitionsTask
func (r *releasePartitionsTask) Timestamp() Timestamp {
	if r.req.Base == nil {
		log.Error("nil base req in releasePartitionsTask", zap.Any("collectionID", r.req.CollectionID))
		return 0
	}
	return r.req.Base.Timestamp
}

func (r *releasePartitionsTask) OnEnqueue() error {
	if r.req == nil || r.req.Base == nil {
		r.SetID(rand.Int63n(100000000000))
	} else {
		r.SetID(r.req.Base.MsgID)
	}
	return nil
}

func (r *releasePartitionsTask) PreExecute(ctx context.Context) error {
	return nil
}

func (r *releasePartitionsTask) Execute(ctx context.Context) error {
	for _, id := range r.req.PartitionIDs {
		err := r.node.loadService.segLoader.replica.removePartition(id)
		if err != nil {
			// not return, try to release all partitions
			log.Error(err.Error())
		}
	}
	return nil
}

func (r *releasePartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}
