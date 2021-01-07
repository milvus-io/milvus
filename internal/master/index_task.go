package master

import (
	"fmt"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

type createIndexTask struct {
	baseTask
	req                 *internalpb.CreateIndexRequest
	indexBuildScheduler *IndexBuildScheduler
	indexLoadScheduler  *IndexLoadScheduler
	segManager          SegmentManager
}

func (task *createIndexTask) Type() internalpb.MsgType {
	return internalpb.MsgType_kCreateIndex
}

func (task *createIndexTask) Ts() (Timestamp, error) {
	return task.req.Timestamp, nil
}

func (task *createIndexTask) Execute() error {
	// modify schema
	if err := task.mt.UpdateFieldIndexParams(task.req.CollectionName, task.req.FieldName, task.req.ExtraParams); err != nil {
		return err
	}
	// check if closed segment has the same index build history
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
	req  *internalpb.DescribeIndexRequest
	resp *servicepb.DescribeIndexResponse
}

func (task *describeIndexTask) Type() internalpb.MsgType {
	return internalpb.MsgType_kDescribeIndex
}

func (task *describeIndexTask) Ts() (Timestamp, error) {
	return task.req.Timestamp, nil
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
	task.resp.ExtraParams = indexParams
	return nil
}

type describeIndexProgressTask struct {
	baseTask
	req          *internalpb.DescribeIndexProgressRequest
	runtimeStats *RuntimeStats
	resp         *servicepb.BoolResponse
}

func (task *describeIndexProgressTask) Type() internalpb.MsgType {
	return internalpb.MsgType_kDescribeIndexProgress
}

func (task *describeIndexProgressTask) Ts() (Timestamp, error) {
	return task.req.Timestamp, nil
}

func (task *describeIndexProgressTask) Execute() error {
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

	task.resp.Value = int64(totalSegmentNums) == relatedSegments
	return nil
}
