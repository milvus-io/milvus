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
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/cdc/core/config"

	"github.com/goccy/go-json"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/cdc/core/model"
	"github.com/milvus-io/milvus/cdc/core/mq/api"
	mqutil "github.com/milvus-io/milvus/cdc/core/mq/util"
	"github.com/milvus-io/milvus/cdc/core/util"
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

type CdcWriterTemplate struct {
	DefaultWriter

	handler    CDCDataHandler
	errProtect *ErrorProtect
	funcMap    map[api.MsgType]func(context.Context, *model.CDCData, WriteCallback)

	bufferConfig             BufferConfig
	bufferLock               sync.Mutex
	currentBufferSize        int64
	bufferOps                []BufferOp
	bufferUpdatePositionFunc NotifyCollectionPositionChangeFunc
	bufferOpsChan            chan []BufferOp

	bufferData     []lo.Tuple2[*model.CDCData, WriteCallback]
	bufferDataChan chan []lo.Tuple2[*model.CDCData, WriteCallback]
}

// NewCdcWriterTemplate options must include HandlerOption
func NewCdcWriterTemplate(options ...config.Option[*CdcWriterTemplate]) CDCWriter {
	c := &CdcWriterTemplate{
		bufferConfig: DefaultBufferConfig,
		errProtect:   FastFail,
	}
	for _, option := range options {
		option.Apply(c)
	}
	c.funcMap = map[api.MsgType]func(context.Context, *model.CDCData, WriteCallback){
		commonpb.MsgType_CreateCollection: c.handleCreateCollection,
		commonpb.MsgType_DropCollection:   c.handleDropCollection,
		commonpb.MsgType_Insert:           c.handleInsert,
		commonpb.MsgType_Delete:           c.handleDelete,
	}
	c.bufferInit()
	return c
}

func (c *CdcWriterTemplate) bufferInit() {
	//c.bufferOpsChan = make(chan []BufferOp)
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

			//bufferOps := <-c.bufferOpsChan
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
			//for _, bufferOp := range bufferOps {
			//	bufferOp.Apply(context.Background(), positionFunc)
			//}
			//if c.bufferUpdatePositionFunc != nil {
			//	for collectionID, collectionPositions := range latestPositions {
			//		for pChannelName, position := range collectionPositions {
			//			c.bufferUpdatePositionFunc(collectionID, collectionNames[collectionID], pChannelName, position)
			//		}
			//	}
			//}

			bufferData := <-c.bufferDataChan
			type CombineData struct {
				param     any
				fails     []func(err error)
				successes []func()
			}
			combineDataMap := make(map[string][]*CombineData)
			combineDataFunc := func(dataArr []lo.Tuple2[*model.CDCData, WriteCallback]) {
				generateKey := func(a string, b string) string {
					return a + ":" + b
				}
				for _, tuple := range dataArr {
					data := tuple.A
					callback := tuple.B
					switch msg := data.Msg.(type) {
					case *api.InsertMsg:
						collectionName := msg.CollectionName
						partitionName := msg.PartitionName
						dataKey := generateKey(collectionName, partitionName)
						// construct columns
						var columns []entity.Column
						isForFail := false
						for _, fieldData := range msg.FieldsData {
							if column, err := entity.FieldDataColumn(fieldData, 0, -1); err == nil {
								columns = append(columns, column)
							} else {
								column, err := entity.FieldDataVector(fieldData)
								if err != nil {
									c.fail("fail to parse the data", err, data, callback)
									isForFail = true
									break
								}
								columns = append(columns, column)
							}
						}
						if isForFail {
							continue
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
									c.success(msg.CollectionID, collectionName, data, callback, positionFunc)
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
							continue
						}
						lastCombineData := combineDataArr[len(combineDataArr)-1]
						insertParam, ok := lastCombineData.param.(*InsertParam)
						// check whether the last data is insert, if not, add the data to array
						if !ok {
							combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
							continue
						}
						// combine the data
						insertParam.Columns = append(insertParam.Columns, columns...)
						lastCombineData.successes = append(lastCombineData.successes, newCombineData.successes...)
						lastCombineData.fails = append(lastCombineData.fails, newCombineData.fails...)
					case *api.DeleteMsg:
						collectionName := msg.CollectionName
						partitionName := msg.PartitionName
						dataKey := generateKey(collectionName, partitionName)
						// get the id column
						column, err := entity.IDColumns(msg.PrimaryKeys, 0, -1)
						if err != nil {
							c.fail("fail to get the id columns", err, data, callback)
							continue
						}
						newCombineData := &CombineData{
							param: &DeleteParam{
								CollectionName: collectionName,
								PartitionName:  partitionName,
								Column:         column,
							},
							successes: []func(){
								func() {
									c.success(msg.CollectionID, collectionName, data, callback, positionFunc)
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
							continue
						}
						lastCombineData := combineDataArr[len(combineDataArr)-1]
						deleteParam, ok := lastCombineData.param.(*DeleteParam)
						// check whether the last data is insert, if not, add the data to array
						if !ok {
							combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
							continue
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
						isForFail := false
						for _, value := range values {
							err = deleteParam.Column.AppendValue(value)
							if err != nil {
								c.fail("fail to combine the delete data", err, data, callback)
								isForFail = true
								break
							}
						}
						if isForFail {
							continue
						}
						lastCombineData.successes = append(lastCombineData.successes, newCombineData.successes...)
						lastCombineData.fails = append(lastCombineData.fails, newCombineData.fails...)
					case *api.DropCollectionMsg:
						collectionName := msg.CollectionName
						dataKey := generateKey(collectionName, "")
						newCombineData := &CombineData{
							param: &DropCollectionParam{
								CollectionName: collectionName,
							},
							successes: []func(){
								func() {
									channelInfos := make(map[string]CallbackChannelInfo)

									collectChannelInfo := func(dropCollectionMsg *api.DropCollectionMsg) {
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
										msgs := msgsValue.([]*api.DropCollectionMsg)
										//if err := json.Unmarshal(util.ToBytes(msgsValue), &msgs); err != nil {
										//	c.fail("fail to drop collection, unmarshal error", err, data, callback)
										//	return
										//}
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
				}
			}
			combineDataFunc(bufferData)

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

	// period flush
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

func (c *CdcWriterTemplate) Write(ctx context.Context, data *model.CDCData, callback WriteCallback) error {
	select {
	case <-c.errProtect.Chan():
		log.Warn("the error protection is triggered", zap.String("protect", c.errProtect.Info()))
		return errors.New("the error protection is triggered")
	default:
	}

	handleFunc, ok := c.funcMap[data.Msg.Type()]
	if !ok {
		log.Warn("not support message type", zap.Any("data", data))
		return fmt.Errorf("not support message type, type: %s", data.Msg.Type().String())
	}
	handleFunc(ctx, data, callback)
	return nil
}

func (c *CdcWriterTemplate) Flush(context context.Context) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	c.clearBufferFunc()
}

func (c *CdcWriterTemplate) handleCreateCollection(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	msg := data.Msg.(*api.CreateCollectionMsg)
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

	var fields []*entity.Field
	lo.ForEach(schema.Fields, func(schema *schemapb.FieldSchema, _ int) {
		fields = append(fields, &entity.Field{
			Name:        schema.Name,
			DataType:    entity.FieldType(schema.DataType),
			PrimaryKey:  schema.IsPrimaryKey,
			AutoID:      schema.AutoID,
			Description: schema.Description,
			TypeParams:  util.ConvertKVPairToMap(schema.TypeParams),
			IndexParams: util.ConvertKVPairToMap(schema.IndexParams),
		})
	})

	entitySchema := &entity.Schema{}
	entitySchema = entitySchema.ReadProto(schema)
	err = c.handler.CreateCollection(ctx, &CreateCollectionParam{
		Schema:           entitySchema,
		ShardsNum:        int32(shardNum),
		ConsistencyLevel: level,
		Properties:       properties,
	})
	if err != nil {
		c.fail("fail to create the collection", err, data, callback)
		return
	}
	callback.OnSuccess(msg.CollectionID, nil)
}

func (c *CdcWriterTemplate) handleDropCollection(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	//msg := data.Msg.(*api.DropCollectionMsg)

	//dropCollectionFunc := BufferOpFunc(func(ctx context.Context, positionFunc NotifyCollectionPositionChangeFunc) {
	//	err := c.handler.DropCollection(ctx, &DropCollectionParam{msg.CollectionName})
	//	if err != nil {
	//		c.fail("fail to drop collection", err, data, callback)
	//		return
	//	}
	//
	//	var channelInfos map[string]CallbackChannelInfo
	//
	//	collectChannelInfo := func(dropCollectionMsg *api.DropCollectionMsg) {
	//		position := dropCollectionMsg.Position()
	//		kd := &commonpb.KeyDataPair{
	//			Key:  position.ChannelName,
	//			Data: position.MsgID,
	//		}
	//		channelInfos[position.ChannelName] = CallbackChannelInfo{
	//			Position: kd,
	//			Ts:       dropCollectionMsg.EndTs(),
	//		}
	//	}
	//	collectChannelInfo(msg)
	//	if msgsValue := data.Extra[model.DropCollectionMsgsKey]; msgsValue != "" {
	//		var msgs []api.TsMsg
	//		if err = json.Unmarshal(util.ToBytes(msgsValue), &msgs); err != nil {
	//			c.fail("fail to drop collection, unmarshal error", err, data, callback)
	//			return
	//		}
	//		for _, tsMsg := range msgs {
	//			otherDropMsg := tsMsg.(*api.DropCollectionMsg)
	//			collectChannelInfo(otherDropMsg)
	//		}
	//	}
	//
	//	callback.OnSuccess(msg.CollectionID, channelInfos)
	//	if positionFunc != nil {
	//		for _, info := range channelInfos {
	//			positionFunc(msg.CollectionID, msg.CollectionName, info.Position.Key, info.Position)
	//		}
	//	}
	//})
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	//c.bufferOps = append(c.bufferOps, dropCollectionFunc)
	c.bufferData = append(c.bufferData, lo.T2(data, callback))
	c.clearBufferFunc()
}

func (c *CdcWriterTemplate) handleInsert(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	msg := data.Msg.(*api.InsertMsg)
	totalSize := mqutil.SizeOfInsertMsg(msg)
	if totalSize < 0 {
		c.fail("fail to get the data size", errors.New("invalid column type"), data, callback)
		return
	}

	//var columns []entity.Column
	//var totalSize int64
	//sizeFunc := func(column entity.Column) bool {
	//	size := SizeColumn(column)
	//	if size < 0 {
	//		c.fail("fail to get the data size", errors.New("invalid column type"), data, callback, zap.String("column_name", column.Name()), zap.String("partition_name", msg.PartitionName))
	//		return false
	//	}
	//	totalSize += SizeColumn(column)
	//	return true
	//}
	//
	//for _, fieldData := range msg.FieldsData {
	//	if column, err := entity.FieldDataColumn(fieldData, 0, -1); err == nil {
	//		columns = append(columns, column)
	//		if !sizeFunc(column) {
	//			return
	//		}
	//	} else {
	//		column, err := entity.FieldDataVector(fieldData)
	//		if err != nil {
	//			c.fail("fail to parse the data", err, data, callback, zap.String("partition_name", msg.PartitionName))
	//			return
	//		}
	//		columns = append(columns, column)
	//		if !sizeFunc(column) {
	//			return
	//		}
	//	}
	//}

	//insertFunc := BufferOpFunc(func(insertCtx context.Context, positionFunc NotifyCollectionPositionChangeFunc) {
	//	err := c.handler.Insert(insertCtx, &InsertParam{
	//		CollectionName: msg.CollectionName,
	//		PartitionName:  msg.PartitionName,
	//		Columns:        columns,
	//	})
	//	if err != nil {
	//		c.fail("fail to insert the data", err, data, callback, zap.String("partition_name", msg.PartitionName))
	//		return
	//	}
	//	c.success(msg.CollectionID, msg.CollectionName, data, callback, positionFunc)
	//})

	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	c.currentBufferSize += totalSize
	//c.bufferOps = append(c.bufferOps, insertFunc)
	c.bufferData = append(c.bufferData, lo.T2(data, callback))
	c.checkBufferSize()
}

func (c *CdcWriterTemplate) handleDelete(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	msg := data.Msg.(*api.DeleteMsg)
	totalSize := mqutil.SizeOfDeleteMsg(msg)

	//var totalSize int64
	//column, err := entity.IDColumns(msg.PrimaryKeys, 0, -1)
	//if err != nil {
	//	c.fail("fail to get the id columns", err, data, callback, zap.String("partition_name", msg.PartitionName))
	//}
	//if totalSize = SizeColumn(column); totalSize < 0 {
	//	c.fail("fail to get the data size", errors.New("invalid column type"), data, callback, zap.String("column_name", column.Name()), zap.String("partition_name", msg.PartitionName))
	//	return
	//}
	//
	//deleteFunc := BufferOpFunc(func(deleteCtx context.Context, positionFunc NotifyCollectionPositionChangeFunc) {
	//	err = c.handler.Delete(deleteCtx, &DeleteParam{
	//		CollectionName: msg.CollectionName,
	//		PartitionName:  msg.PartitionName,
	//		Column:         column,
	//	})
	//	if err != nil {
	//		c.fail("fail to delete the column", err, data, callback, zap.String("partition_name", msg.PartitionName))
	//		return
	//	}
	//	c.success(msg.CollectionID, msg.CollectionName, data, callback, positionFunc)
	//})
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	c.currentBufferSize += totalSize
	//c.bufferOps = append(c.bufferOps, deleteFunc)
	c.bufferData = append(c.bufferData, lo.T2(data, callback))
	c.checkBufferSize()
}

func (c *CdcWriterTemplate) collectionName(data *model.CDCData) string {
	f, ok := data.Msg.(interface{ GetCollectionName() string })
	if ok {
		return f.GetCollectionName()
	}
	return ""
}

func (c *CdcWriterTemplate) partitionName(data *model.CDCData) string {
	f, ok := data.Msg.(interface{ GetPartitionName() string })
	if ok {
		return f.GetPartitionName()
	}
	return ""
}

func (c *CdcWriterTemplate) fail(msg string, err error, data *model.CDCData,
	callback WriteCallback, field ...zap.Field) {

	log.Warn(msg, append(field,
		zap.String("collection_name", c.collectionName(data)),
		zap.String("partition_name", c.partitionName(data)),
		zap.Error(err))...)
	callback.OnFail(data, errors.WithMessage(err, msg))
	c.errProtect.Inc()
}

func (c *CdcWriterTemplate) success(collectionID int64, collectionName string, data *model.CDCData, callback WriteCallback, positionFunc NotifyCollectionPositionChangeFunc) {
	position := data.Msg.Position()
	kd := &commonpb.KeyDataPair{
		Key:  position.ChannelName,
		Data: position.MsgID,
	}
	callback.OnSuccess(collectionID, map[string]CallbackChannelInfo{
		position.ChannelName: {
			Position: kd,
			Ts:       data.Msg.EndTs(),
		},
	})
	if positionFunc != nil {
		positionFunc(collectionID, collectionName, position.ChannelName, kd)
	}
}

func (c *CdcWriterTemplate) checkBufferSize() {
	if c.currentBufferSize >= c.bufferConfig.Size {
		c.clearBufferFunc()
	}
}

func (c *CdcWriterTemplate) clearBufferFunc() {
	// no copy, is a shallow copy
	//c.bufferOpsChan <- c.bufferOps[:]
	c.bufferDataChan <- c.bufferData[:]
	//c.bufferOps = []BufferOp{}
	c.bufferData = []lo.Tuple2[*model.CDCData, WriteCallback]{}
	c.currentBufferSize = 0
}

func SizeColumn(column entity.Column) int64 {
	var data any
	switch column.(type) {
	case *entity.ColumnBool:
		data = column.(*entity.ColumnBool).Data()
	case *entity.ColumnInt8:
		data = column.(*entity.ColumnInt8).Data()
	case *entity.ColumnInt16:
		data = column.(*entity.ColumnInt16).Data()
	case *entity.ColumnInt32:
		data = column.(*entity.ColumnInt32).Data()
	case *entity.ColumnInt64:
		data = column.(*entity.ColumnInt64).Data()
	case *entity.ColumnFloat:
		data = column.(*entity.ColumnFloat).Data()
	case *entity.ColumnDouble:
		data = column.(*entity.ColumnDouble).Data()
	case *entity.ColumnString:
		strArr := column.(*entity.ColumnString).Data()
		total := 0
		for _, s := range strArr {
			total += binary.Size(util.ToBytes(s))
		}
		return int64(total)
	case *entity.ColumnVarChar:
		strArr := column.(*entity.ColumnVarChar).Data()
		total := 0
		for _, s := range strArr {
			total += binary.Size(util.ToBytes(s))
		}
		return int64(total)
	case *entity.ColumnBinaryVector:
		byteArr := column.(*entity.ColumnBinaryVector).Data()
		total := 0
		for _, s := range byteArr {
			total += binary.Size(s)
		}
		return int64(total)
	case *entity.ColumnFloatVector:
		floatArr := column.(*entity.ColumnFloatVector).Data()
		total := 0
		for _, f := range floatArr {
			total += binary.Size(f)
		}
		return int64(total)
	default:
		return -1
	}
	return int64(binary.Size(data))
}
