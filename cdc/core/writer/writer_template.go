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

package writer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/goccy/go-json"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/cdc/core/config"
	"github.com/milvus-io/milvus/cdc/core/model"
	"github.com/milvus-io/milvus/cdc/core/util"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

var log = util.Log

type BufferConfig struct {
	Period time.Duration
	Size   int64
}

var DefaultBufferConfig = BufferConfig{
	Period: 1 * time.Minute,
	Size:   1024 * 1024,
}

var NoBufferConfig = BufferConfig{
	Period: 0,
	Size:   -1,
}

type CDCWriterTemplate struct {
	DefaultWriter

	handler    CDCDataHandler
	errProtect *ErrorProtect
	funcMap    map[msgstream.MsgType]func(context.Context, *model.CDCData, WriteCallback)

	bufferConfig             BufferConfig
	bufferLock               sync.Mutex
	currentBufferSize        int64
	bufferOps                []BufferOp
	bufferUpdatePositionFunc NotifyCollectionPositionChangeFunc
	bufferOpsChan            chan []BufferOp

	bufferData     []lo.Tuple2[*model.CDCData, WriteCallback]
	bufferDataChan chan []lo.Tuple2[*model.CDCData, WriteCallback]
}

// NewCDCWriterTemplate options must include HandlerOption
func NewCDCWriterTemplate(options ...config.Option[*CDCWriterTemplate]) CDCWriter {
	c := &CDCWriterTemplate{
		bufferConfig: DefaultBufferConfig,
		errProtect:   FastFail(),
	}
	for _, option := range options {
		option.Apply(c)
	}
	c.funcMap = map[msgstream.MsgType]func(context.Context, *model.CDCData, WriteCallback){
		commonpb.MsgType_CreateCollection: c.handleCreateCollection,
		commonpb.MsgType_DropCollection:   c.handleDropCollection,
		commonpb.MsgType_Insert:           c.handleInsert,
		commonpb.MsgType_Delete:           c.handleDelete,
	}
	c.initBuffer()
	c.periodFlush()
	return c
}

func (c *CDCWriterTemplate) initBuffer() {
	c.bufferDataChan = make(chan []lo.Tuple2[*model.CDCData, WriteCallback])

	// execute buffer ops
	go func() {
		for {
			select {
			case <-c.errProtect.Chan():
				log.Warn("the error protection is triggered", zap.String("protect", c.errProtect.Info()))
				return
			default:
			}

			latestPositions := make(map[int64]map[string]*commonpb.KeyDataPair)
			collectionNames := make(map[int64]string)
			positionFunc := NotifyCollectionPositionChangeFunc(func(collectionID int64, collectionName string, pChannelName string, position *commonpb.KeyDataPair) {
				if position == nil {
					return
				}
				collectionNames[collectionID] = collectionName
				collectionPositions, ok := latestPositions[collectionID]
				if !ok {
					collectionPositions = make(map[string]*commonpb.KeyDataPair)
					latestPositions[collectionID] = collectionPositions
				}
				collectionPositions[pChannelName] = position
			})

			bufferData := <-c.bufferDataChan
			combineDataMap := make(map[string][]*CombineData)
			c.combineDataFunc(bufferData, combineDataMap, positionFunc)
			executeSuccesses := func(successes []func()) {
				for _, success := range successes {
					success()
				}
			}
			executeFails := func(fails []func(err error), err error) {
				for _, fail := range fails {
					fail(err)
				}
			}

			for _, combineDatas := range combineDataMap {
				for _, combineData := range combineDatas {
					var err error
					switch p := combineData.param.(type) {
					case *InsertParam:
						err = c.handler.Insert(context.Background(), p)
					case *DeleteParam:
						err = c.handler.Delete(context.Background(), p)
					case *DropCollectionParam:
						err = c.handler.DropCollection(context.Background(), p)
					default:
						log.Warn("invalid param", zap.Any("data", combineData))
						continue
					}
					if err != nil {
						executeFails(combineData.fails, err)
						continue
					}
					executeSuccesses(combineData.successes)
				}
			}

			if c.bufferUpdatePositionFunc != nil {
				for collectionID, collectionPositions := range latestPositions {
					for pChannelName, position := range collectionPositions {
						c.bufferUpdatePositionFunc(collectionID, collectionNames[collectionID], pChannelName, position)
					}
				}
			}
		}
	}()
}

type CombineData struct {
	param     any
	fails     []func(err error)
	successes []func()
}

func (c *CDCWriterTemplate) combineDataFunc(dataArr []lo.Tuple2[*model.CDCData, WriteCallback],
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc) {

	for _, tuple := range dataArr {
		data := tuple.A
		callback := tuple.B
		switch msg := data.Msg.(type) {
		case *msgstream.InsertMsg:
			c.handleInsertBuffer(msg, data, callback, combineDataMap, positionFunc)
		case *msgstream.DeleteMsg:
			c.handleDeleteBuffer(msg, data, callback, combineDataMap, positionFunc)
		case *msgstream.DropCollectionMsg:
			c.handleDropCollectionBuffer(msg, data, callback, combineDataMap, positionFunc)
		}
	}
}

func (c *CDCWriterTemplate) generateBufferKey(a string, b string) string {
	return a + ":" + b
}

func (c *CDCWriterTemplate) handleInsertBuffer(msg *msgstream.InsertMsg,
	data *model.CDCData, callback WriteCallback,
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc,
) {

	collectionName := msg.CollectionName
	partitionName := msg.PartitionName
	dataKey := c.generateBufferKey(collectionName, partitionName)
	// construct columns
	var columns []entity.Column
	for _, fieldData := range msg.FieldsData {
		if column, err := entity.FieldDataColumn(fieldData, 0, -1); err == nil {
			columns = append(columns, column)
		} else {
			column, err := entity.FieldDataVector(fieldData)
			if err != nil {
				c.fail("fail to parse the data", err, data, callback)
				return
			}
			columns = append(columns, column)
		}
	}
	// new combine data for convenient usage below
	newCombineData := &CombineData{
		param: &InsertParam{
			CollectionName: collectionName,
			PartitionName:  partitionName,
			Columns:        columns,
		},
		successes: []func(){
			func() {
				c.success(msg.CollectionID, collectionName, len(msg.RowIDs), data, callback, positionFunc)
			},
		},
		fails: []func(err error){
			func(err error) {
				c.fail("fail to insert the data", err, data, callback)
			},
		},
	}
	combineDataArr, ok := combineDataMap[dataKey]
	// check whether the combineDataMap contains the key, if not, add the data
	if !ok {
		combineDataMap[dataKey] = []*CombineData{
			newCombineData,
		}
		return
	}
	lastCombineData := combineDataArr[len(combineDataArr)-1]
	insertParam, ok := lastCombineData.param.(*InsertParam)
	// check whether the last data is insert, if not, add the data to array
	if !ok {
		combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
		return
	}
	// combine the data
	if err := c.preCombineColumn(insertParam.Columns, columns); err != nil {
		c.fail("fail to combine the data", err, data, callback)
		return
	}
	c.combineColumn(insertParam.Columns, columns)
	lastCombineData.successes = append(lastCombineData.successes, newCombineData.successes...)
	lastCombineData.fails = append(lastCombineData.fails, newCombineData.fails...)
}

func (c *CDCWriterTemplate) handleDeleteBuffer(msg *msgstream.DeleteMsg,
	data *model.CDCData, callback WriteCallback,
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc,
) {
	collectionName := msg.CollectionName
	partitionName := msg.PartitionName
	dataKey := c.generateBufferKey(collectionName, partitionName)
	// get the id column
	column, err := entity.IDColumns(msg.PrimaryKeys, 0, -1)
	if err != nil {
		c.fail("fail to get the id columns", err, data, callback)
		return
	}
	newCombineData := &CombineData{
		param: &DeleteParam{
			CollectionName: collectionName,
			PartitionName:  partitionName,
			Column:         column,
		},
		successes: []func(){
			func() {
				c.success(msg.CollectionID, collectionName, column.Len(), data, callback, positionFunc)
			},
		},
		fails: []func(err error){
			func(err error) {
				c.fail("fail to delete the column", err, data, callback)
			},
		},
	}
	combineDataArr, ok := combineDataMap[dataKey]
	// check whether the combineDataMap contains the key, if not, add the data
	if !ok {
		combineDataMap[dataKey] = []*CombineData{
			newCombineData,
		}
		return
	}
	lastCombineData := combineDataArr[len(combineDataArr)-1]
	deleteParam, ok := lastCombineData.param.(*DeleteParam)
	// check whether the last data is insert, if not, add the data to array
	if !ok {
		combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
		return
	}
	// combine the data
	var values []interface{}
	switch columnValue := column.(type) {
	case *entity.ColumnInt64:
		for _, id := range columnValue.Data() {
			values = append(values, id)
		}
	case *entity.ColumnVarChar:
		for _, varchar := range columnValue.Data() {
			values = append(values, varchar)
		}
	default:
		c.fail("fail to combine the delete data", err, data, callback)
	}
	for _, value := range values {
		err = deleteParam.Column.AppendValue(value)
		if err != nil {
			c.fail("fail to combine the delete data", err, data, callback)
			return
		}
	}
	lastCombineData.successes = append(lastCombineData.successes, newCombineData.successes...)
	lastCombineData.fails = append(lastCombineData.fails, newCombineData.fails...)
}

func (c *CDCWriterTemplate) handleDropCollectionBuffer(msg *msgstream.DropCollectionMsg,
	data *model.CDCData, callback WriteCallback,
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc,
) {
	collectionName := msg.CollectionName
	dataKey := c.generateBufferKey(collectionName, "")
	newCombineData := &CombineData{
		param: &DropCollectionParam{
			CollectionName: collectionName,
		},
		successes: []func(){
			func() {
				channelInfos := make(map[string]CallbackChannelInfo)

				collectChannelInfo := func(dropCollectionMsg *msgstream.DropCollectionMsg) {
					position := dropCollectionMsg.Position()
					kd := &commonpb.KeyDataPair{
						Key:  position.ChannelName,
						Data: position.MsgID,
					}
					channelInfos[position.ChannelName] = CallbackChannelInfo{
						Position: kd,
						Ts:       dropCollectionMsg.EndTs(),
					}
				}
				collectChannelInfo(msg)
				if msgsValue := data.Extra[model.DropCollectionMsgsKey]; msgsValue != nil {
					msgs := msgsValue.([]*msgstream.DropCollectionMsg)
					for _, tsMsg := range msgs {
						collectChannelInfo(tsMsg)
					}
				}

				callback.OnSuccess(msg.CollectionID, channelInfos)
				if positionFunc != nil {
					for _, info := range channelInfos {
						positionFunc(msg.CollectionID, msg.CollectionName, info.Position.Key, info.Position)
					}
				}
			},
		},
		fails: []func(err error){
			func(err error) {
				c.fail("fail to drop collection", err, data, callback)
			},
		},
	}
	combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
}

func (c *CDCWriterTemplate) periodFlush() {
	go func() {
		if c.bufferConfig.Period <= 0 {
			return
		}
		ticker := time.NewTicker(c.bufferConfig.Period)
		for {
			select {
			case <-ticker.C:
				c.Flush(context.Background())
			}
		}
	}()
}

func (c *CDCWriterTemplate) Write(ctx context.Context, data *model.CDCData, callback WriteCallback) error {
	select {
	case <-c.errProtect.Chan():
		log.Warn("the error protection is triggered", zap.String("protect", c.errProtect.Info()))
		return errors.New("the error protection is triggered")
	default:
	}

	handleFunc, ok := c.funcMap[data.Msg.Type()]
	if !ok {
		// don't execute the fail callback, because the future messages will be ignored and don't trigger the error protection
		log.Warn("not support message type", zap.Any("data", data))
		return fmt.Errorf("not support message type, type: %s", data.Msg.Type().String())
	}
	handleFunc(ctx, data, callback)
	return nil
}

func (c *CDCWriterTemplate) Flush(context context.Context) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	c.clearBufferFunc()
}

func (c *CDCWriterTemplate) handleCreateCollection(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	msg := data.Msg.(*msgstream.CreateCollectionMsg)
	schema := &schemapb.CollectionSchema{}
	err := json.Unmarshal(msg.Schema, schema)
	if err != nil {
		c.fail("fail to unmarshal the collection schema", err, data, callback)
		return
	}
	var shardNum int32 = 0
	if value, ok := data.Extra[model.ShardNumKey]; ok {
		shardNum = value.(int32)
	}
	level := commonpb.ConsistencyLevel_Strong
	if value, ok := data.Extra[model.ConsistencyLevelKey]; ok {
		level = value.(commonpb.ConsistencyLevel)
	}
	var properties []*commonpb.KeyValuePair
	if value, ok := data.Extra[model.CollectionPropertiesKey]; ok {
		properties = value.([]*commonpb.KeyValuePair)
	}

	entitySchema := &entity.Schema{}
	entitySchema = entitySchema.ReadProto(schema)
	err = c.handler.CreateCollection(ctx, &CreateCollectionParam{
		Schema:           entitySchema,
		ShardsNum:        shardNum,
		ConsistencyLevel: level,
		Properties:       properties,
	})
	if err != nil {
		c.fail("fail to create the collection", err, data, callback)
		return
	}
	callback.OnSuccess(msg.CollectionID, nil)
}

func (c *CDCWriterTemplate) handleDropCollection(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	c.bufferData = append(c.bufferData, lo.T2(data, callback))
	c.clearBufferFunc()
}

func (c *CDCWriterTemplate) handleInsert(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	msg := data.Msg.(*msgstream.InsertMsg)
	totalSize := SizeOfInsertMsg(msg)
	if totalSize < 0 {
		c.fail("fail to get the data size", errors.New("invalid column type"), data, callback)
		return
	}

	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	c.currentBufferSize += totalSize
	c.bufferData = append(c.bufferData, lo.T2(data, callback))
	c.checkBufferSize()
}

func (c *CDCWriterTemplate) handleDelete(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	msg := data.Msg.(*msgstream.DeleteMsg)
	totalSize := SizeOfDeleteMsg(msg)

	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	c.currentBufferSize += totalSize
	c.bufferData = append(c.bufferData, lo.T2(data, callback))
	c.checkBufferSize()
}

func (c *CDCWriterTemplate) collectionName(data *model.CDCData) string {
	f, ok := data.Msg.(interface{ GetCollectionName() string })
	if ok {
		return f.GetCollectionName()
	}
	return ""
}

func (c *CDCWriterTemplate) partitionName(data *model.CDCData) string {
	f, ok := data.Msg.(interface{ GetPartitionName() string })
	if ok {
		return f.GetPartitionName()
	}
	return ""
}

func (c *CDCWriterTemplate) fail(msg string, err error, data *model.CDCData,
	callback WriteCallback, field ...zap.Field) {

	log.Warn(msg, append(field,
		zap.String("collection_name", c.collectionName(data)),
		zap.String("partition_name", c.partitionName(data)),
		zap.Error(err))...)
	callback.OnFail(data, errors.WithMessage(err, msg))
	c.errProtect.Inc()
}

func (c *CDCWriterTemplate) success(collectionID int64, collectionName string, rowCount int,
	data *model.CDCData, callback WriteCallback, positionFunc NotifyCollectionPositionChangeFunc) {
	position := data.Msg.Position()
	kd := &commonpb.KeyDataPair{
		Key:  position.ChannelName,
		Data: position.MsgID,
	}
	callback.OnSuccess(collectionID, map[string]CallbackChannelInfo{
		position.ChannelName: {
			Position:    kd,
			MsgType:     data.Msg.Type(),
			MsgRowCount: rowCount,
			Ts:          data.Msg.EndTs(),
		},
	})
	if positionFunc != nil {
		positionFunc(collectionID, collectionName, position.ChannelName, kd)
	}
}

func (c *CDCWriterTemplate) checkBufferSize() {
	if c.currentBufferSize >= c.bufferConfig.Size {
		c.clearBufferFunc()
	}
}

func (c *CDCWriterTemplate) clearBufferFunc() {
	// no copy, is a shallow copy
	c.bufferDataChan <- c.bufferData[:]
	c.bufferData = []lo.Tuple2[*model.CDCData, WriteCallback]{}
	c.currentBufferSize = 0
}

func (c *CDCWriterTemplate) isSupportType(fieldType entity.FieldType) bool {
	return fieldType == entity.FieldTypeBool ||
		fieldType == entity.FieldTypeInt8 ||
		fieldType == entity.FieldTypeInt16 ||
		fieldType == entity.FieldTypeInt32 ||
		fieldType == entity.FieldTypeInt64 ||
		fieldType == entity.FieldTypeFloat ||
		fieldType == entity.FieldTypeDouble ||
		fieldType == entity.FieldTypeString ||
		fieldType == entity.FieldTypeVarChar ||
		fieldType == entity.FieldTypeBinaryVector ||
		fieldType == entity.FieldTypeFloatVector
}

func (c *CDCWriterTemplate) preCombineColumn(a []entity.Column, b []entity.Column) error {
	for i := range a {
		if a[i].Type() != b[i].Type() || !c.isSupportType(b[i].Type()) {
			log.Warn("fail to combine the column",
				zap.Any("a", a[i].Type()), zap.Any("b", b[i].Type()))
			return errors.New("fail to combine the column")
		}
	}
	return nil
}

// combineColumn the b will be added to a. before execute the method, MUST execute the preCombineColumn
func (c *CDCWriterTemplate) combineColumn(a []entity.Column, b []entity.Column) {
	for i := range a {
		var values []interface{}
		switch columnValue := b[i].(type) {
		case *entity.ColumnBool:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnInt8:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnInt16:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnInt32:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnInt64:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnFloat:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnDouble:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnString:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnVarChar:
			for _, varchar := range columnValue.Data() {
				values = append(values, varchar)
			}
		case *entity.ColumnBinaryVector:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnFloatVector:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		default:
			log.Panic("not support column type", zap.Any("value", columnValue))
		}
		for _, value := range values {
			_ = a[i].AppendValue(value)
		}
	}
}
