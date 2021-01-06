package master

import (
	"context"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"

	"github.com/zilliztech/milvus-distributed/internal/errors"
)

type FlushScheduler struct {
	client              WriteNodeClient
	metaTable           *metaTable
	segmentFlushChan    chan UniqueID
	segmentDescribeChan chan UniqueID
	indexBuilderSch     persistenceScheduler

	ctx                context.Context
	cancel             context.CancelFunc
	globalTSOAllocator func() (Timestamp, error)
}

func NewFlushScheduler(ctx context.Context, client WriteNodeClient, metaTable *metaTable, buildScheduler *IndexBuildScheduler, globalTSOAllocator func() (Timestamp, error)) *FlushScheduler {
	ctx2, cancel := context.WithCancel(ctx)

	return &FlushScheduler{
		client:              client,
		metaTable:           metaTable,
		indexBuilderSch:     buildScheduler,
		segmentFlushChan:    make(chan UniqueID, 100),
		segmentDescribeChan: make(chan UniqueID, 100),
		ctx:                 ctx2,
		cancel:              cancel,
		globalTSOAllocator:  globalTSOAllocator,
	}
}

func (scheduler *FlushScheduler) schedule(id interface{}) error {
	segmentID := id.(UniqueID)
	segmentMeta, err := scheduler.metaTable.GetSegmentByID(segmentID)
	if err != nil {
		return err
	}

	ts, err := scheduler.globalTSOAllocator()
	if err != nil {
		return err
	}
	// todo set corrent timestamp
	err = scheduler.client.FlushSegment(segmentID, segmentMeta.CollectionID, segmentMeta.PartitionTag, ts)
	log.Printf("flush segment %d", segmentID)
	if err != nil {
		return err
	}

	scheduler.segmentDescribeChan <- segmentID

	return nil
}
func (scheduler *FlushScheduler) describe() error {
	timeTick := time.Tick(100 * time.Millisecond)
	descTasks := make(map[UniqueID]bool)
	closable := make([]UniqueID, 0)
	for {
		select {
		case <-scheduler.ctx.Done():
			{
				log.Printf("broadcast context done, exit")
				return errors.New("broadcast done exit")
			}
		case <-timeTick:
			for singleSegmentID := range descTasks {
				description, err := scheduler.client.DescribeSegment(singleSegmentID)
				if err != nil {
					log.Printf("describe segment %d err %s", singleSegmentID, err.Error())
					continue
				}
				if !description.IsClosed {
					continue
				}

				log.Printf("flush segment %d is closed", singleSegmentID)
				mapData, err := scheduler.client.GetInsertBinlogPaths(singleSegmentID)
				if err != nil {
					log.Printf("get insert binlog paths err, segID: %d, err: %s", singleSegmentID, err.Error())
					continue
				}
				segMeta, err := scheduler.metaTable.GetSegmentByID(singleSegmentID)
				if err != nil {
					log.Printf("get segment from metable failed, segID: %d, err: %s", singleSegmentID, err.Error())
					continue
				}
				for fieldID, data := range mapData {
					// check field indexable
					indexable, err := scheduler.metaTable.IsIndexable(segMeta.CollectionID, fieldID)
					if err != nil {
						log.Printf("check field indexable from meta table failed, collID: %d, fieldID: %d, err %s", segMeta.CollectionID, fieldID, err.Error())
						continue
					}
					if !indexable {
						continue
					}
					info := &IndexBuildInfo{
						segmentID:      singleSegmentID,
						fieldID:        fieldID,
						binlogFilePath: data,
					}
					err = scheduler.indexBuilderSch.Enqueue(info)
					log.Printf("segment %d field %d enqueue build index scheduler", singleSegmentID, fieldID)
					if err != nil {
						log.Printf("index build enqueue failed, %s", err.Error())
						continue
					}
				}
				// Save data to meta table
				segMeta.BinlogFilePaths = make([]*etcdpb.FieldBinlogFiles, 0)
				for k, v := range mapData {
					segMeta.BinlogFilePaths = append(segMeta.BinlogFilePaths, &etcdpb.FieldBinlogFiles{
						FieldID:     k,
						BinlogFiles: v,
					})
				}
				if err = scheduler.metaTable.UpdateSegment(segMeta); err != nil {
					return err
				}
				log.Printf("flush segment %d finished", singleSegmentID)
				closable = append(closable, singleSegmentID)
			}

			// remove closed segment and clear closable
			for _, segID := range closable {
				delete(descTasks, segID)
			}
			closable = closable[:0]
		case segID := <-scheduler.segmentDescribeChan:
			descTasks[segID] = false
		}
	}

}

func (scheduler *FlushScheduler) scheduleLoop() {
	for {
		select {
		case id := <-scheduler.segmentFlushChan:
			err := scheduler.schedule(id)
			if err != nil {
				log.Println(err)
			}
		case <-scheduler.ctx.Done():
			log.Print("server is closed, exit flush scheduler loop")
			return
		}
	}
}

func (scheduler *FlushScheduler) Enqueue(id interface{}) error {
	scheduler.segmentFlushChan <- id.(UniqueID)
	return nil
}

func (scheduler *FlushScheduler) Start() error {
	go scheduler.scheduleLoop()
	go scheduler.describe()
	return nil
}

func (scheduler *FlushScheduler) Close() {
	scheduler.cancel()
}
