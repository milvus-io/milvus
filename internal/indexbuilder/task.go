package indexbuilder

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexbuilderpb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

type task interface {
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	PreExecute() error
	Execute() error
	PostExecute() error
	WaitToFinish() error
	Notify(err error)
	OnEnqueue() error
}

type BaseTask struct {
	done  chan error
	ctx   context.Context
	id    UniqueID
	table *metaTable
}

func (bt *BaseTask) ID() UniqueID {
	return bt.id
}

func (bt *BaseTask) setID(id UniqueID) {
	bt.id = id
}

func (bt *BaseTask) WaitToFinish() error {
	select {
	case <-bt.ctx.Done():
		return errors.New("timeout")
	case err := <-bt.done:
		return err
	}
}

func (bt *BaseTask) Notify(err error) {
	bt.done <- err
}

type IndexAddTask struct {
	BaseTask
	req         *indexbuilderpb.BuildIndexRequest
	indexID     UniqueID
	idAllocator *allocator.IDAllocator
	buildQueue  TaskQueue
	kv          kv.Base
}

func (it *IndexAddTask) SetID(ID UniqueID) {
	it.BaseTask.setID(ID)
}

func (it *IndexAddTask) OnEnqueue() error {
	var err error
	it.indexID, err = it.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	return nil
}

func (it *IndexAddTask) PreExecute() error {
	log.Println("pretend to check Index Req")
	err := it.table.AddIndex(it.indexID, it.req)
	if err != nil {
		return err
	}
	return nil
}

func (it *IndexAddTask) Execute() error {
	t := newIndexBuildTask()
	t.table = it.table
	t.indexID = it.indexID
	t.kv = it.kv
	var cancel func()
	t.ctx, cancel = context.WithTimeout(it.ctx, reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-t.ctx.Done():
			return errors.New("index add timeout")
		default:
			return it.buildQueue.Enqueue(t)
		}
	}
	return fn()
}

func (it *IndexAddTask) PostExecute() error {
	return nil
}

func NewIndexAddTask() *IndexAddTask {
	return &IndexAddTask{
		BaseTask: BaseTask{
			done: make(chan error),
		},
	}
}

type IndexBuildTask struct {
	BaseTask
	index     Index
	indexID   UniqueID
	kv        kv.Base
	savePaths []string
	indexMeta *indexbuilderpb.IndexMeta
}

func newIndexBuildTask() *IndexBuildTask {
	return &IndexBuildTask{
		BaseTask: BaseTask{
			done: make(chan error, 1), // intend to do this
		},
	}
}

func (it *IndexBuildTask) SetID(ID UniqueID) {
	it.BaseTask.setID(ID)
}

func (it *IndexBuildTask) OnEnqueue() error {
	return it.table.UpdateIndexEnqueTime(it.indexID, time.Now())
}

func (it *IndexBuildTask) PreExecute() error {
	return it.table.UpdateIndexScheduleTime(it.indexID, time.Now())
}

func (it *IndexBuildTask) Execute() error {
	err := it.table.UpdateIndexStatus(it.indexID, indexbuilderpb.IndexStatus_INPROGRESS)
	if err != nil {
		return err
	}

	typeParams := make(map[string]string)
	for _, kvPair := range it.indexMeta.Req.GetTypeParams() {
		key, value := kvPair.GetKey(), kvPair.GetValue()
		_, ok := typeParams[key]
		if ok {
			return errors.New("duplicated key in type params")
		}
		typeParams[key] = value
	}

	indexParams := make(map[string]string)
	for _, kvPair := range it.indexMeta.Req.GetIndexParams() {
		key, value := kvPair.GetKey(), kvPair.GetValue()
		_, ok := indexParams[key]
		if ok {
			return errors.New("duplicated key in index params")
		}
		indexParams[key] = value
	}

	it.index, err = NewCIndex(typeParams, indexParams)
	if err != nil {
		return err
	}

	getKeyByPathNaive := func(path string) string {
		// splitElements := strings.Split(path, "/")
		// return splitElements[len(splitElements)-1]
		return path
	}
	getValueByPath := func(path string) ([]byte, error) {
		data, err := it.kv.Load(path)
		if err != nil {
			return nil, err
		}
		return []byte(data), nil
	}
	getBlobByPath := func(path string) (*Blob, error) {
		value, err := getValueByPath(path)
		if err != nil {
			return nil, err
		}
		return &Blob{
			Key:   getKeyByPathNaive(path),
			Value: value,
		}, nil
	}
	getStorageBlobs := func(blobs []*Blob) []*storage.Blob {
		return blobs
	}

	toLoadDataPaths := it.indexMeta.Req.GetDataPaths()
	keys := make([]string, 0)
	blobs := make([]*Blob, 0)
	for _, path := range toLoadDataPaths {
		keys = append(keys, getKeyByPathNaive(path))
		blob, err := getBlobByPath(path)
		if err != nil {
			return err
		}
		blobs = append(blobs, blob)
	}

	storageBlobs := getStorageBlobs(blobs)
	var insertCodec storage.InsertCodec
	partitionID, segmentID, insertData, err := insertCodec.Deserialize(storageBlobs)
	if len(insertData.Data) != 1 {
		return errors.New("we expect only one field in deserialized insert data")
	}

	for _, value := range insertData.Data {
		// TODO: BinaryVectorFieldData
		floatVectorFieldData, ok := value.(*storage.FloatVectorFieldData)
		if !ok {
			return errors.New("we expect FloatVectorFieldData or BinaryVectorFieldData")
		}

		err = it.index.BuildFloatVecIndex(floatVectorFieldData.Data)
		if err != nil {
			return err
		}

		indexBlobs, err := it.index.Serialize()
		if err != nil {
			return err
		}

		var indexCodec storage.IndexCodec
		serializedIndexBlobs, err := indexCodec.Serialize(getStorageBlobs(indexBlobs))
		if err != nil {
			return err
		}

		getSavePathByKey := func(key string) string {
			// TODO: fix me, use more reasonable method
			return strconv.Itoa(int(it.indexID)) + "/" + strconv.Itoa(int(partitionID)) + "/" + strconv.Itoa(int(segmentID)) + "/" + key
		}
		saveBlob := func(path string, value []byte) error {
			return it.kv.Save(path, string(value))
		}

		it.savePaths = make([]string, 0)
		for _, blob := range serializedIndexBlobs {
			key, value := blob.Key, blob.Value
			savePath := getSavePathByKey(key)
			err := saveBlob(savePath, value)
			if err != nil {
				return err
			}
			it.savePaths = append(it.savePaths, savePath)
		}
	}

	return it.index.Delete()
}

func (it *IndexBuildTask) PostExecute() error {
	return it.table.CompleteIndex(it.indexID, it.savePaths)
}
