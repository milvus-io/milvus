package indexnode

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

const (
	paramsKeyToParse = "params"
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
	done chan error
	ctx  context.Context
	id   UniqueID
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

type IndexBuildTask struct {
	BaseTask
	index         Index
	kv            kv.Base
	savePaths     []string
	cmd           *indexpb.BuildIndexCmd
	serviceClient typeutil.IndexServiceInterface
	nodeID        UniqueID
}

func newIndexBuildTask() *IndexBuildTask {
	ctx := context.Background()
	return &IndexBuildTask{
		BaseTask: BaseTask{
			ctx:  ctx,
			done: make(chan error), // intend to do this
		},
	}
}

func (it *IndexBuildTask) SetID(ID UniqueID) {
	it.BaseTask.setID(ID)
}

func (it *IndexBuildTask) OnEnqueue() error {
	it.SetID(it.cmd.IndexBuildID)
	log.Printf("[IndexBuilderTask] Enqueue TaskID: %v", it.ID())
	return nil
}

func (it *IndexBuildTask) PreExecute() error {
	log.Println("preExecute...")
	return nil
}

func (it *IndexBuildTask) PostExecute() error {
	log.Println("PostExecute...")
	var err error
	defer func() {
		if err != nil {
			it.Rollback()
		}
	}()

	if it.serviceClient == nil {
		err = errors.New("IndexBuildTask, serviceClient is nil")
		return err
	}

	nty := &indexpb.BuildIndexNotification{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		IndexBuildID:   it.cmd.IndexBuildID,
		NodeID:         it.nodeID,
		IndexFilePaths: it.savePaths,
	}

	resp, err := it.serviceClient.NotifyBuildIndex(nty)
	if err != nil {
		log.Println("IndexBuildTask notify err:", err.Error())
		return err
	}

	if resp.ErrorCode != commonpb.ErrorCode_SUCCESS {
		err = errors.New(resp.Reason)
	}
	return err
}

func (it *IndexBuildTask) Rollback() error {

	if it.savePaths == nil {
		return nil
	}

	err := it.kv.MultiRemove(it.savePaths)
	if err != nil {
		log.Println("IndexBuildTask Rollback Failed:", err.Error())
		return err
	}
	return nil
}

func (it *IndexBuildTask) Execute() error {
	log.Println("start build index ...")
	var err error

	log.Println("type params: ", it.cmd.Req.GetTypeParams())
	log.Println("index params: ", it.cmd.Req.GetIndexParams())

	typeParams := make(map[string]string)
	for _, kvPair := range it.cmd.Req.GetTypeParams() {
		key, value := kvPair.GetKey(), kvPair.GetValue()
		_, ok := typeParams[key]
		if ok {
			return errors.New("duplicated key in type params")
		}
		if key == paramsKeyToParse {
			params, err := funcutil.ParseIndexParamsMap(value)
			if err != nil {
				return err
			}
			for pk, pv := range params {
				typeParams[pk] = pv
			}
		} else {
			typeParams[key] = value
		}
	}

	indexParams := make(map[string]string)
	for _, kvPair := range it.cmd.Req.GetIndexParams() {
		key, value := kvPair.GetKey(), kvPair.GetValue()
		_, ok := indexParams[key]
		if ok {
			return errors.New("duplicated key in index params")
		}
		if key == paramsKeyToParse {
			params, err := funcutil.ParseIndexParamsMap(value)
			if err != nil {
				return err
			}
			for pk, pv := range params {
				indexParams[pk] = pv
			}
		} else {
			indexParams[key] = value
		}
	}

	it.index, err = NewCIndex(typeParams, indexParams)
	if err != nil {
		fmt.Println("NewCIndex err:", err.Error())
		return err
	}
	defer func() {
		err = it.index.Delete()
		if err != nil {
			log.Print("CIndexDelete Failed")
		}
	}()

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

	toLoadDataPaths := it.cmd.Req.GetDataPaths()
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
	partitionID, segmentID, insertData, err2 := insertCodec.Deserialize(storageBlobs)
	//fmt.Println("IndexBuilder for segmentID,", segmentID)
	if err2 != nil {
		return err2
	}
	if len(insertData.Data) != 1 {
		return errors.New("we expect only one field in deserialized insert data")
	}

	for _, value := range insertData.Data {
		// TODO: BinaryVectorFieldData
		floatVectorFieldData, fOk := value.(*storage.FloatVectorFieldData)
		if fOk {
			err = it.index.BuildFloatVecIndexWithoutIds(floatVectorFieldData.Data)
			if err != nil {
				fmt.Println("BuildFloatVecIndexWithoutIds, error:", err.Error())
				return err
			}
		}

		binaryVectorFieldData, bOk := value.(*storage.BinaryVectorFieldData)
		if bOk {
			err = it.index.BuildBinaryVecIndexWithoutIds(binaryVectorFieldData.Data)
			if err != nil {
				fmt.Println("BuildBinaryVecIndexWithoutIds, err:", err.Error())
				return err
			}
		}

		if !fOk && !bOk {
			return errors.New("we expect FloatVectorFieldData or BinaryVectorFieldData")
		}

		indexBlobs, err := it.index.Serialize()
		if err != nil {
			fmt.Println("serialize ... err:", err.Error())

			return err
		}

		var indexCodec storage.IndexCodec
		serializedIndexBlobs, err := indexCodec.Serialize(getStorageBlobs(indexBlobs), indexParams, it.cmd.Req.IndexName, it.cmd.Req.IndexID)
		if err != nil {
			return err
		}

		getSavePathByKey := func(key string) string {
			// TODO: fix me, use more reasonable method
			return strconv.Itoa(int(it.cmd.IndexBuildID)) + "/" + strconv.Itoa(int(partitionID)) + "/" + strconv.Itoa(int(segmentID)) + "/" + key
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
	// err = it.index.Delete()
	// if err != nil {
	// 	log.Print("CIndexDelete Failed")
	// }
	return nil
}
