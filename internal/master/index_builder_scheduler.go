package master

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
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
	typeParams, err := scheduler.metaTable.GetFieldTypeParams(segMeta.CollectionID, indexBuildInfo.fieldID)
	if err != nil {
		return err
	}
	indexParams, err := scheduler.metaTable.GetFieldIndexParams(segMeta.CollectionID, indexBuildInfo.fieldID)

	if err != nil {
		return err
	}
	typeParamsMap := make(map[string]string)
	indexParamsMap := make(map[string]string)
	for _, kv := range typeParams {
		typeParamsMap[kv.Key] = kv.Value
	}
	for _, kv := range indexParams {
		indexParamsMap[kv.Key] = kv.Value
	}

	parseMap := func(mStr string) (map[string]string, error) {
		buffer := make(map[string]interface{})
		err := json.Unmarshal([]byte(mStr), &buffer)
		if err != nil {
			return nil, errors.New("Unmarshal params failed")
		}
		ret := make(map[string]string)
		for key, value := range buffer {
			valueStr := fmt.Sprintf("%v", value)
			ret[key] = valueStr
		}
		return ret, nil
	}
	var typeParamsKV []*commonpb.KeyValuePair
	for key := range typeParamsMap {
		if key == "params" {
			mapParams, err := parseMap(typeParamsMap[key])
			if err != nil {
				log.Println("parse params error: ", err)
			}
			for pk, pv := range mapParams {
				typeParamsKV = append(typeParamsKV, &commonpb.KeyValuePair{
					Key:   pk,
					Value: pv,
				})
			}
		} else {
			typeParamsKV = append(typeParamsKV, &commonpb.KeyValuePair{
				Key:   key,
				Value: typeParamsMap[key],
			})
		}
	}

	var indexParamsKV []*commonpb.KeyValuePair
	for key := range indexParamsMap {
		if key == "params" {
			mapParams, err := parseMap(indexParamsMap[key])
			if err != nil {
				log.Println("parse params error: ", err)
			}
			for pk, pv := range mapParams {
				indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
					Key:   pk,
					Value: pv,
				})
			}
		} else {
			indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
				Key:   key,
				Value: indexParamsMap[key],
			})
		}
	}

	requset := &indexpb.BuildIndexRequest{
		DataPaths:   indexBuildInfo.binlogFilePath,
		TypeParams:  typeParamsKV,
		IndexParams: indexParamsKV,
	}

	indexResp, err := scheduler.client.BuildIndex(requset)
	if err != nil {
		log.Printf("build index for segment %d field %d, failed:%s", indexBuildInfo.segmentID, indexBuildInfo.fieldID, err.Error())
		return err
	}
	indexID := indexResp.IndexID

	err = scheduler.metaTable.AddFieldIndexMeta(&etcdpb.FieldIndexMeta{
		SegmentID:   indexBuildInfo.segmentID,
		FieldID:     indexBuildInfo.fieldID,
		IndexID:     indexID,
		IndexParams: indexParams,
		State:       commonpb.IndexState_NONE,
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
				indexIDs := []UniqueID{channelInfo.id}
				request := &indexpb.IndexStatesRequest{
					IndexIDs: indexIDs,
				}
				description, err := scheduler.client.GetIndexStates(request)
				if err != nil {
					return err
				}
				if description.States[0].State == commonpb.IndexState_FINISHED {
					log.Printf("build index for segment %d field %d is finished", indexBuildInfo.segmentID, indexBuildInfo.fieldID)
					request := &indexpb.IndexFilePathsRequest{
						IndexIDs: indexIDs,
					}

					response, err := scheduler.client.GetIndexFilePaths(request)
					if err != nil {
						return err
					}
					var filePathsInfos [][]string
					for _, indexID := range indexIDs {
						for _, filePathInfo := range response.FilePaths {
							if indexID == filePathInfo.IndexID {
								filePathsInfos = append(filePathsInfos, filePathInfo.IndexFilePaths)
								break
							}
						}
					}
					filePaths := filePathsInfos[0]

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
						indexParams:    channelInfo.indexParams,
					}
					// Save data to meta table
					err = scheduler.metaTable.UpdateFieldIndexMeta(&etcdpb.FieldIndexMeta{
						SegmentID:      indexBuildInfo.segmentID,
						FieldID:        indexBuildInfo.fieldID,
						IndexID:        indexID,
						IndexParams:    channelInfo.indexParams,
						State:          commonpb.IndexState_FINISHED,
						IndexFilePaths: filePaths,
					})
					if err != nil {
						fmt.Println("indexbuilder scheduler updateFiledIndexMetaFailed", indexBuildInfo.segmentID)
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
						State:       description.States[0].State,
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
