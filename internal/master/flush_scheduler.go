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

	ctx    context.Context
	cancel context.CancelFunc
}

func NewFlushScheduler(ctx context.Context, client WriteNodeClient, metaTable *metaTable, buildScheduler *IndexBuildScheduler) *FlushScheduler {
	ctx2, cancel := context.WithCancel(ctx)

	return &FlushScheduler{
		client:              client,
		metaTable:           metaTable,
		indexBuilderSch:     buildScheduler,
		segmentFlushChan:    make(chan UniqueID, 100),
		segmentDescribeChan: make(chan UniqueID, 100),
		ctx:                 ctx2,
		cancel:              cancel,
	}
}

func (scheduler *FlushScheduler) schedule(id interface{}) error {
	segmentID := id.(UniqueID)
	err := scheduler.client.FlushSegment(segmentID)
	log.Printf("flush segment %d", segmentID)
	if err != nil {
		return err
	}

	scheduler.segmentDescribeChan <- segmentID

	return nil
}
func (scheduler *FlushScheduler) describe() error {
	for {
		select {
		case <-scheduler.ctx.Done():
			{
				log.Printf("broadcast context done, exit")
				return errors.New("broadcast done exit")
			}
		case singleSegmentID := <-scheduler.segmentDescribeChan:
			for {
				description, err := scheduler.client.DescribeSegment(singleSegmentID)
				if err != nil {
					return err
				}
				if description.IsClosed {
					log.Printf("flush segment %d is closed", singleSegmentID)
					mapData, err := scheduler.client.GetInsertBinlogPaths(singleSegmentID)
					if err != nil {
						return err
					}
					for fieldID, data := range mapData {
						// check field indexable
						segMeta, err := scheduler.metaTable.GetSegmentByID(singleSegmentID)
						if err != nil {
							return err
						}
						indexable, err := scheduler.metaTable.IsIndexable(segMeta.CollectionID, fieldID)
						if err != nil {
							return err
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
							return err
						}
					}
					// Save data to meta table
					segMeta, err := scheduler.metaTable.GetSegmentByID(singleSegmentID)
					if err != nil {
						return err
					}
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
					break
				}
				time.Sleep(1 * time.Second)
			}
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
