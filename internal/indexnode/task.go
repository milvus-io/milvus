// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package indexnode

import (
	"context"
	"errors"
	"runtime"
	"strconv"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

const (
	paramsKeyToParse   = "params"
	IndexBuildTaskName = "IndexBuildTask"
)

type task interface {
	Ctx() context.Context
	ID() UniqueID // return ReqID
	Name() string
	SetID(uid UniqueID) // set ReqID
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
	OnEnqueue() error
	SetError(err error)
}

type BaseTask struct {
	done chan error
	ctx  context.Context
	id   UniqueID
	err  error
}

func (bt *BaseTask) SetError(err error) {
	bt.err = err
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
	kv            kv.BaseKV
	savePaths     []string
	req           *indexpb.BuildIndexRequest
	serviceClient types.IndexService
	nodeID        UniqueID
}

func (it *IndexBuildTask) Ctx() context.Context {
	return it.ctx
}

func (it *IndexBuildTask) ID() UniqueID {
	return it.id
}

func (it *IndexBuildTask) SetID(ID UniqueID) {
	it.BaseTask.setID(ID)
}

func (bt *BaseTask) Name() string {
	return IndexBuildTaskName
}

func (it *IndexBuildTask) OnEnqueue() error {
	it.SetID(it.req.IndexBuildID)
	log.Debug("indexnode", zap.Int64("[IndexBuilderTask] Enqueue TaskID", it.ID()))
	return nil
}

func (it *IndexBuildTask) PreExecute(ctx context.Context) error {
	log.Debug("preExecute...")
	return nil
}

func (it *IndexBuildTask) PostExecute(ctx context.Context) error {
	log.Debug("PostExecute...")

	defer func() {
		if it.err != nil {
			it.Rollback()
		}
	}()

	if it.serviceClient == nil {
		err := errors.New("IndexBuildTask, serviceClient is nil")
		log.Debug("[IndexBuildTask][PostExecute] serviceClient is nil")
		return err
	}

	nty := &indexpb.NotifyBuildIndexRequest{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IndexBuildID:   it.req.IndexBuildID,
		NodeID:         it.nodeID,
		IndexFilePaths: it.savePaths,
	}
	if it.err != nil {
		nty.Status.ErrorCode = commonpb.ErrorCode_BuildIndexError
	}

	ctx = context.TODO()
	resp, err := it.serviceClient.NotifyBuildIndex(ctx, nty)
	if err != nil {
		log.Warn("indexnode", zap.String("error", err.Error()))
		return err
	}

	if resp.ErrorCode != commonpb.ErrorCode_Success {
		err = errors.New(resp.Reason)
		log.Debug("indexnode", zap.String("[IndexBuildTask][PostExecute] err", err.Error()))
	}
	return err
}

func (it *IndexBuildTask) Execute(ctx context.Context) error {
	log.Debug("start build index ...")
	var err error

	typeParams := make(map[string]string)
	for _, kvPair := range it.req.GetTypeParams() {
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
	for _, kvPair := range it.req.GetIndexParams() {
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
		log.Error("indexnode", zap.String("NewCIndex err:", err.Error()))
		return err
	}
	defer func() {
		err = it.index.Delete()
		if err != nil {
			log.Warn("CIndexDelete Failed")
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

	toLoadDataPaths := it.req.GetDataPaths()
	keys := make([]string, len(toLoadDataPaths))
	blobs := make([]*Blob, len(toLoadDataPaths))

	loadKey := func(idx int) error {
		keys[idx] = getKeyByPathNaive(toLoadDataPaths[idx])
		blob, err := getBlobByPath(toLoadDataPaths[idx])
		if err != nil {
			return err
		}

		blobs[idx] = blob

		return nil
	}
	err = funcutil.ProcessFuncParallel(len(toLoadDataPaths), runtime.NumCPU(), loadKey, "loadKey")
	if err != nil {
		return err
	}

	storageBlobs := getStorageBlobs(blobs)
	var insertCodec storage.InsertCodec
	defer insertCodec.Close()
	partitionID, segmentID, insertData, err2 := insertCodec.Deserialize(storageBlobs)
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
				log.Error("indexnode", zap.String("BuildFloatVecIndexWithoutIds error", err.Error()))
				return err
			}
		}

		binaryVectorFieldData, bOk := value.(*storage.BinaryVectorFieldData)
		if bOk {
			err = it.index.BuildBinaryVecIndexWithoutIds(binaryVectorFieldData.Data)
			if err != nil {
				log.Error("indexnode", zap.String("BuildBinaryVecIndexWithoutIds err", err.Error()))
				return err
			}
		}

		if !fOk && !bOk {
			return errors.New("we expect FloatVectorFieldData or BinaryVectorFieldData")
		}

		indexBlobs, err := it.index.Serialize()
		if err != nil {
			log.Error("indexnode", zap.String("serialize err", err.Error()))

			return err
		}

		var indexCodec storage.IndexCodec
		serializedIndexBlobs, err := indexCodec.Serialize(getStorageBlobs(indexBlobs), indexParams, it.req.IndexName, it.req.IndexID)
		if err != nil {
			return err
		}

		getSavePathByKey := func(key string) string {
			// TODO: fix me, use more reasonable method
			return strconv.Itoa(int(it.req.IndexBuildID)) + "/" + strconv.Itoa(int(partitionID)) + "/" + strconv.Itoa(int(segmentID)) + "/" + key
		}
		saveBlob := func(path string, value []byte) error {
			return it.kv.Save(path, string(value))
		}

		it.savePaths = make([]string, len(serializedIndexBlobs))
		saveIndexFile := func(idx int) error {
			blob := serializedIndexBlobs[idx]
			key, value := blob.Key, blob.Value

			savePath := getSavePathByKey(key)

			err := saveBlob(savePath, value)
			if err != nil {
				return err
			}

			it.savePaths[idx] = savePath

			return nil
		}
		err = funcutil.ProcessFuncParallel(len(serializedIndexBlobs), runtime.NumCPU(), saveIndexFile, "saveIndexFile")
		if err != nil {
			return err
		}
	}
	// err = it.index.Delete()
	// if err != nil {
	// 	log.Print("CIndexDelete Failed")
	// }
	return nil
}
func (it *IndexBuildTask) Rollback() error {

	if it.savePaths == nil {
		return nil
	}

	err := it.kv.MultiRemove(it.savePaths)
	if err != nil {
		log.Warn("indexnode", zap.String("IndexBuildTask Rollback Failed", err.Error()))
		return err
	}
	return nil
}
