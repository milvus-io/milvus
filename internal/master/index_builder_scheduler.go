package master

import (
	"context"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexbuilderpb"
)

type IndexBuildInfo struct {
	segmentID      UniqueID
	fieldID        UniqueID
	binlogFilePath []string
}
type IndexBuildChannelInfo struct {
	id          UniqueID
	info        *IndexBuildInfo
	indexParams []*commonpb.KeyValuePair
}

type IndexBuildScheduler struct {
	client          BuildIndexClient
	metaTable       *metaTable
	indexBuildChan  chan *IndexBuildInfo
	indexLoadSch    persistenceScheduler
	indexDescribeID chan UniqueID
	indexDescribe   chan *IndexBuildChannelInfo

	ctx    context.Context
	cancel context.CancelFunc
}

func NewIndexBuildScheduler(ctx context.Context, client BuildIndexClient, metaTable *metaTable, indexLoadScheduler *IndexLoadScheduler) *IndexBuildScheduler {
	ctx2, cancel := context.WithCancel(ctx)

	return &IndexBuildScheduler{
		client:         client,
		metaTable:      metaTable,
		indexLoadSch:   indexLoadScheduler,
		indexBuildChan: make(chan *IndexBuildInfo, 100),
		indexDescribe:  make(chan *IndexBuildChannelInfo, 100),
		ctx:            ctx2,
		cancel:         cancel,
	}
}

func (scheduler *IndexBuildScheduler) schedule(info interface{}) error {
	indexBuildInfo := info.(*IndexBuildInfo)
	segMeta, err := scheduler.metaTable.GetSegmentByID(indexBuildInfo.segmentID)
	if err != nil {
		return err
	}

	// parse index params
	indexParams, err := scheduler.metaTable.GetFieldIndexParams(segMeta.CollectionID, indexBuildInfo.fieldID)
	if err != nil {
		return err
	}
	indexParamsMap := make(map[string]string)
	for _, kv := range indexParams {
		indexParamsMap[kv.Key] = kv.Value
	}

	indexID, err := scheduler.client.BuildIndexWithoutID(indexBuildInfo.binlogFilePath, nil, indexParamsMap)
	log.Printf("build index for segment %d field %d", indexBuildInfo.segmentID, indexBuildInfo.fieldID)
	if err != nil {
		return err
	}

	err = scheduler.metaTable.AddFieldIndexMeta(&etcdpb.FieldIndexMeta{
		SegmentID:   indexBuildInfo.segmentID,
		FieldID:     indexBuildInfo.fieldID,
		IndexID:     indexID,
		IndexParams: indexParams,
		Status:      indexbuilderpb.IndexStatus_NONE,
	})
	if err != nil {
		log.Printf("WARNING: " + err.Error())
		//return err
	}

	scheduler.indexDescribe <- &IndexBuildChannelInfo{
		id:          indexID,
		info:        indexBuildInfo,
		indexParams: indexParams,
	}
	return nil
}

func (scheduler *IndexBuildScheduler) describe() error {
	for {
		select {
		case <-scheduler.ctx.Done():
			{
				log.Printf("broadcast context done, exit")
				return errors.New("broadcast done exit")
			}
		case channelInfo := <-scheduler.indexDescribe:
			indexID := channelInfo.id
			indexBuildInfo := channelInfo.info
			for {
				description, err := scheduler.client.DescribeIndex(channelInfo.id)
				if err != nil {
					return err
				}
				if description.Status == indexbuilderpb.IndexStatus_FINISHED {
					log.Printf("build index for segment %d field %d is finished", indexBuildInfo.segmentID, indexBuildInfo.fieldID)
					filePaths, err := scheduler.client.GetIndexFilePaths(indexID)
					if err != nil {
						return err
					}

					//TODO: remove fileName
					var fieldName string
					segMeta := scheduler.metaTable.segID2Meta[indexBuildInfo.segmentID]
					collMeta := scheduler.metaTable.collID2Meta[segMeta.CollectionID]
					if collMeta.Schema != nil {
						for _, field := range collMeta.Schema.Fields {
							if field.FieldID == indexBuildInfo.fieldID {
								fieldName = field.Name
							}
						}
					}

					info := &IndexLoadInfo{
						segmentID:      indexBuildInfo.segmentID,
						fieldID:        indexBuildInfo.fieldID,
						fieldName:      fieldName,
						indexFilePaths: filePaths,
					}
					// Save data to meta table
					err = scheduler.metaTable.UpdateFieldIndexMeta(&etcdpb.FieldIndexMeta{
						SegmentID:      indexBuildInfo.segmentID,
						FieldID:        indexBuildInfo.fieldID,
						IndexID:        indexID,
						IndexParams:    channelInfo.indexParams,
						Status:         indexbuilderpb.IndexStatus_FINISHED,
						IndexFilePaths: filePaths,
					})
					if err != nil {
						return err
					}

					err = scheduler.indexLoadSch.Enqueue(info)
					log.Printf("build index for segment %d field %d enqueue load index", indexBuildInfo.segmentID, indexBuildInfo.fieldID)
					if err != nil {
						return err
					}
					log.Printf("build index for segment %d field %d finished", indexBuildInfo.segmentID, indexBuildInfo.fieldID)
					break
				} else {
					// save status to meta table
					err = scheduler.metaTable.UpdateFieldIndexMeta(&etcdpb.FieldIndexMeta{
						SegmentID:   indexBuildInfo.segmentID,
						FieldID:     indexBuildInfo.fieldID,
						IndexID:     indexID,
						IndexParams: channelInfo.indexParams,
						Status:      description.Status,
					})
					if err != nil {
						return err
					}
				}
				time.Sleep(1 * time.Second)
			}
		}
	}

}

func (scheduler *IndexBuildScheduler) scheduleLoop() {
	for {
		select {
		case info := <-scheduler.indexBuildChan:
			err := scheduler.schedule(info)
			if err != nil {
				log.Println(err)
			}
		case <-scheduler.ctx.Done():
			log.Print("server is closed, exit index build loop")
			return
		}
	}
}

func (scheduler *IndexBuildScheduler) Enqueue(info interface{}) error {
	scheduler.indexBuildChan <- info.(*IndexBuildInfo)
	return nil
}

func (scheduler *IndexBuildScheduler) Start() error {
	go scheduler.scheduleLoop()
	go scheduler.describe()
	return nil
}

func (scheduler *IndexBuildScheduler) Close() {
	scheduler.cancel()
}
