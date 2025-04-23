// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pipeline

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// TODO support set EmbddingType
// type EmbeddingType int32

type embeddingNode struct {
	BaseNode

	schema      *schemapb.CollectionSchema
	pkField     *schemapb.FieldSchema
	channelName string

	// embeddingType EmbeddingType
	functionRunners map[int64]function.FunctionRunner
}

func newEmbeddingNode(channelName string, schema *schemapb.CollectionSchema) (*embeddingNode, error) {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(paramtable.Get().DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32())
	baseNode.SetMaxParallelism(paramtable.Get().DataNodeCfg.FlowGraphMaxParallelism.GetAsInt32())

	node := &embeddingNode{
		BaseNode:        baseNode,
		channelName:     channelName,
		schema:          schema,
		functionRunners: make(map[int64]function.FunctionRunner),
	}

	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() {
			node.pkField = field
			break
		}
	}

	for _, tf := range schema.GetFunctions() {
		functionRunner, err := function.NewFunctionRunner(schema, tf)
		if err != nil {
			return nil, err
		}
		if functionRunner == nil {
			continue
		}
		node.functionRunners[tf.GetId()] = functionRunner
	}
	return node, nil
}

func (eNode *embeddingNode) Name() string {
	return fmt.Sprintf("embeddingNode-%s", eNode.channelName)
}

func (eNode *embeddingNode) bm25Embedding(runner function.FunctionRunner, inputFieldId, outputFieldId int64, data *storage.InsertData, meta map[int64]*storage.BM25Stats) error {
	if _, ok := meta[outputFieldId]; !ok {
		meta[outputFieldId] = storage.NewBM25Stats()
	}

	embeddingData, ok := data.Data[inputFieldId].GetDataRows().([]string)
	if !ok {
		return errors.New("BM25 embedding failed: input field data not varchar/text")
	}

	output, err := runner.BatchRun(embeddingData)
	if err != nil {
		return err
	}

	sparseArray, ok := output[0].(*schemapb.SparseFloatArray)
	if !ok {
		return errors.New("BM25 embedding failed: BM25 runner output not sparse map")
	}

	meta[outputFieldId].AppendBytes(sparseArray.GetContents()...)
	data.Data[outputFieldId] = BuildSparseFieldData(sparseArray)
	return nil
}

func (eNode *embeddingNode) embedding(datas []*storage.InsertData) (map[int64]*storage.BM25Stats, error) {
	meta := make(map[int64]*storage.BM25Stats)
	for _, data := range datas {
		for _, functionRunner := range eNode.functionRunners {
			functionSchema := functionRunner.GetSchema()
			switch functionSchema.GetType() {
			case schemapb.FunctionType_BM25:
				err := eNode.bm25Embedding(functionRunner, functionSchema.GetInputFieldIds()[0], functionSchema.GetOutputFieldIds()[0], data, meta)
				if err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("unknown function type %s", functionSchema.Type)
			}
		}
	}
	return meta, nil
}

func (eNode *embeddingNode) Embedding(datas []*writebuffer.InsertData) error {
	for _, data := range datas {
		stats, err := eNode.embedding(data.GetDatas())
		if err != nil {
			return err
		}
		data.SetBM25Stats(stats)
	}
	return nil
}

func (eNode *embeddingNode) Operate(in []Msg) []Msg {
	fgMsg := in[0].(*FlowGraphMsg)

	if fgMsg.IsCloseMsg() {
		return []Msg{fgMsg}
	}

	insertData, err := writebuffer.PrepareInsert(eNode.schema, eNode.pkField, fgMsg.InsertMessages)
	if err != nil {
		log.Error("failed to prepare insert data", zap.Error(err))
		panic(err)
	}

	err = eNode.Embedding(insertData)
	if err != nil {
		log.Warn("failed to embedding insert data", zap.Error(err))
		panic(err)
	}

	fgMsg.InsertData = insertData
	return []Msg{fgMsg}
}

func BuildSparseFieldData(array *schemapb.SparseFloatArray) storage.FieldData {
	return &storage.SparseFloatVectorFieldData{
		SparseFloatArray: schemapb.SparseFloatArray{
			Contents: array.GetContents(),
			Dim:      array.GetDim(),
		},
	}
}
