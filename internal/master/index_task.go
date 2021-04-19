package master

import (
	"fmt"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
)

type createIndexTask struct {
	baseTask
	req                 *milvuspb.CreateIndexRequest
	indexBuildScheduler *IndexBuildScheduler
	indexLoadScheduler  *IndexLoadScheduler
	segManager          SegmentManager
}

func (task *createIndexTask) Type() commonpb.MsgType {
	return commonpb.MsgType_kCreateIndex
}

func (task *createIndexTask) Ts() (Timestamp, error) {
	return task.req.Base.Timestamp, nil
}

func (task *createIndexTask) Execute() error {
	collMeta, err := task.mt.GetCollectionByName(task.req.CollectionName)
	if err != nil {
		return err
	}
	var fieldID int64 = -1
	for _, fieldSchema := range collMeta.Schema.Fields {
		if fieldSchema.Name == task.req.FieldName {
			fieldID = fieldSchema.FieldID
			break
		}
	}
	if fieldID == -1 {
		return fmt.Errorf("can not find field name %s", task.req.FieldName)
	}

	// pre checks
	isIndexable, err := task.mt.IsIndexable(collMeta.ID, fieldID)
	if err != nil {
		return err
	}
	if !isIndexable {
		return fmt.Errorf("field %s is not vector", task.req.FieldName)
	}

	// modify schema
	if err := task.mt.UpdateFieldIndexParams(task.req.CollectionName, task.req.FieldName, task.req.ExtraParams); err != nil {
		return err
	}
	// check if closed segment has the same index build history
	for _, segID := range collMeta.SegmentIDs {
		segMeta, err := task.mt.GetSegmentByID(segID)
		if err != nil {
			return err
		}
		if segMeta.CloseTime == 0 {
			continue
		}
		hasIndexMeta, err := task.mt.HasFieldIndexMeta(segID, fieldID, task.req.ExtraParams)
		if err != nil {
			return err
		}

		if hasIndexMeta {
			// load index
			indexMeta, err := task.mt.GetFieldIndexMeta(segID, fieldID, task.req.ExtraParams)
			if err != nil {
				return err
			}
			err = task.indexLoadScheduler.Enqueue(&IndexLoadInfo{
				segmentID:      segID,
				fieldID:        fieldID,
				fieldName:      task.req.FieldName,
				indexFilePaths: indexMeta.IndexFilePaths,
				indexParams:    indexMeta.IndexParams,
			})
			if err != nil {
				return err
			}
		} else {
			// create index
			for _, kv := range segMeta.BinlogFilePaths {
				if kv.FieldID != fieldID {
					continue
				}
				err := task.indexBuildScheduler.Enqueue(&IndexBuildInfo{
					segmentID:      segID,
					fieldID:        fieldID,
					binlogFilePath: kv.BinlogFiles,
				})
				if err != nil {
					return err
				}
				break
			}
		}
	}

	// close unfilled segment
	return task.segManager.ForceClose(collMeta.ID)
}

type describeIndexTask struct {
	baseTask
	req  *milvuspb.DescribeIndexRequest
	resp *milvuspb.DescribeIndexResponse
}

func (task *describeIndexTask) Type() commonpb.MsgType {
	return commonpb.MsgType_kDescribeIndex
}

func (task *describeIndexTask) Ts() (Timestamp, error) {
	return task.req.Base.Timestamp, nil
}

func (task *describeIndexTask) Execute() error {
	collMeta, err := task.mt.GetCollectionByName(task.req.CollectionName)
	if err != nil {
		return err
	}

	var fieldID int64 = -1
	for _, fieldSchema := range collMeta.Schema.Fields {
		if fieldSchema.Name == task.req.FieldName {
			fieldID = fieldSchema.FieldID
			break
		}
	}
	if fieldID == -1 {
		return fmt.Errorf("can not find field %s", task.req.FieldName)
	}
	indexParams, err := task.mt.GetFieldIndexParams(collMeta.ID, fieldID)
	if err != nil {
		return err
	}
	description := &milvuspb.IndexDescription{
		IndexName: "", // todo add IndexName to master meta_table
		Params:    indexParams,
	}
	task.resp.IndexDescriptions = []*milvuspb.IndexDescription{description}
	return nil
}

type getIndexStateTask struct {
	baseTask
	req          *milvuspb.IndexStateRequest
	runtimeStats *RuntimeStats
	resp         *milvuspb.IndexStateResponse
}

func (task *getIndexStateTask) Type() commonpb.MsgType {
	return commonpb.MsgType_kGetIndexState
}

func (task *getIndexStateTask) Ts() (Timestamp, error) {
	return task.req.Base.Timestamp, nil
}

func (task *getIndexStateTask) Execute() error {
	// get field id, collection id
	collMeta, err := task.mt.GetCollectionByName(task.req.CollectionName)
	if err != nil {
		return err
	}

	var fieldID int64 = -1
	for _, fieldSchema := range collMeta.Schema.Fields {
		if fieldSchema.Name == task.req.FieldName {
			fieldID = fieldSchema.FieldID
			break
		}
	}
	if fieldID == -1 {
		return fmt.Errorf("can not find field %s", task.req.FieldName)
	}

	// total segment nums
	totalSegmentNums := len(collMeta.SegmentIDs)

	indexParams, err := task.mt.GetFieldIndexParams(collMeta.ID, fieldID)
	if err != nil {
		return err
	}

	// get completed segment nums from querynode's runtime stats
	relatedSegments := task.runtimeStats.GetTotalNumOfRelatedSegments(collMeta.ID, fieldID, indexParams)
	task.resp = &milvuspb.IndexStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}
	if int64(totalSegmentNums) == relatedSegments {
		task.resp.State = commonpb.IndexState_FINISHED
	} else {
		task.resp.State = commonpb.IndexState_INPROGRESS
	}
	return nil
}
