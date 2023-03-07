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

package reader

import (
	"context"
	"errors"
	"math/rand"
	"path"
	"strconv"
	"sync"

	"github.com/goccy/go-json"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/cdc/core/config"
	"github.com/milvus-io/milvus/cdc/core/model"
	"github.com/milvus-io/milvus/cdc/core/pb"
	"github.com/milvus-io/milvus/cdc/core/util"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var log = util.Log

type CollectionInfo struct {
	collectionName string
	positions      map[string]*commonpb.KeyDataPair
}

type MilvusCollectionReader struct {
	DefaultReader

	etcdConfig  config.MilvusEtcdConfig
	mqConfig    config.MilvusMQConfig
	collections []CollectionInfo
	monitor     Monitor
	dataChanLen int

	etcdCli            util.KVApi
	factoryCreator     FactoryCreator
	allCollectionNames []string
	dataChan           chan *model.CDCData
	cancelWatch        context.CancelFunc
	closeStreamFuncs   []func()

	startOnce sync.Once
	quitOnce  sync.Once
	isQuit    util.Value[bool]

	// Please no read or write it excluding in the beginning of readStreamData method
	readingSteamCollection []int64
	readingLock            sync.Mutex
}

func NewMilvusCollectionReader(options ...config.Option[*MilvusCollectionReader]) (*MilvusCollectionReader, error) {
	reader := &MilvusCollectionReader{
		monitor:        NewDefaultMonitor(),
		factoryCreator: NewDefaultFactoryCreator(),
		dataChanLen:    10,
	}
	for _, option := range options {
		option.Apply(reader)
	}
	var err error
	reader.etcdCli, err = util.GetEtcdClient(reader.etcdConfig.Endpoints)
	if err != nil {
		log.Warn("fail to get etcd client", zap.Error(err))
		return nil, err
	}
	reader.dataChan = make(chan *model.CDCData, reader.dataChanLen)
	reader.allCollectionNames = lo.Map(reader.collections, func(t CollectionInfo, _ int) string {
		return t.collectionName
	})
	reader.isQuit.Store(false)
	return reader, nil
}

func (reader *MilvusCollectionReader) StartRead(ctx context.Context) <-chan *model.CDCData {
	reader.startOnce.Do(func() {
		var (
			watchCtx, cancel = context.WithCancel(context.Background())
			wg               = &sync.WaitGroup{}
		)
		reader.cancelWatch = cancel

		wg.Add(len(reader.allCollectionNames))
		reader.goWatchCollection(watchCtx)
		reader.goGetAllCollections(wg)
	})

	return reader.dataChan
}

func (reader *MilvusCollectionReader) goWatchCollection(watchCtx context.Context) {
	go func() {
		// watch collection prefix to avoid new collection while getting the all collection
		// TODO improvement watch single instance
		watchChan := reader.etcdCli.Watch(watchCtx, reader.collectionPrefix()+"/", clientv3.WithPrefix())
		for {
			select {
			case watchResp, ok := <-watchChan:
				if !ok {
					reader.monitor.WatchChanClosed()
					return
				}
				lo.ForEach(watchResp.Events, func(event *clientv3.Event, _ int) {
					if event.Type == clientv3.EventTypePut {
						collectionKey := util.ToString(event.Kv.Key)
						info := &pb.CollectionInfo{}
						err := proto.Unmarshal(event.Kv.Value, info)
						if err != nil {
							log.Warn("fail to unmarshal the collection info", zap.String("key", collectionKey), zap.Error(err))
							reader.monitor.OnFailUnKnowCollection(collectionKey, err)
							return
						}
						getCollectionState := func(i *pb.CollectionInfo) pb.CollectionState {
							return i.State
						}
						if getCollectionState(info) == pb.CollectionState_CollectionCreated {
							go func() {
								if lo.Contains(reader.allCollectionNames, reader.collectionName(info)) {
									err := util.Do(context.Background(), func() error {
										err := reader.fillCollectionField(info)
										if err != nil {
											log.Info("fail to get collection fields, retry...", zap.String("key", collectionKey), zap.Error(err))
										}
										return err
									})
									if err != nil {
										log.Warn("fail to get collection fields", zap.String("key", collectionKey), zap.Error(err))
										reader.monitor.OnFailGetCollectionInfo(info.ID, reader.collectionName(info), err)
										return
									}
									reader.readStreamData(info, true)
								}
							}()
						}
					}
				})
			case <-watchCtx.Done():
				return
			}
		}
	}()
}

func (reader *MilvusCollectionReader) goGetAllCollections(wg *sync.WaitGroup) {
	go func() {
		var (
			existedCollectionInfos []*pb.CollectionInfo
			err                    error
		)

		existedCollectionInfos, err = reader.getCollectionInfo(reader.allCollectionNames)
		if err != nil {
			log.Warn("fail to get collection", zap.Error(err))
			reader.monitor.OnFailUnKnowCollection(reader.collectionPrefix(), err)
		}

		for _, info := range existedCollectionInfos {
			wg.Done()
			go reader.readStreamData(info, false)
		}
	}()
	go func() {
		wg.Wait()
		reader.monitor.OnSuccessGetAllCollectionInfo()
	}()
}

func (reader *MilvusCollectionReader) collectionPrefix() string {
	c := reader.etcdConfig
	return util.GetCollectionPrefix(c.RootPath, c.MetaSubPath, c.CollectionKey)
}

func (reader *MilvusCollectionReader) fieldPrefix() string {
	c := reader.etcdConfig
	return util.GetFieldPrefix(c.RootPath, c.MetaSubPath, c.FiledKey)
}

func (reader *MilvusCollectionReader) collectionName(info *pb.CollectionInfo) string {
	return info.Schema.Name
}

// getCollectionInfo The return value meanings are respectively:
// 1. collection infos that the collection have existed
// 2. error message
func (reader *MilvusCollectionReader) getCollectionInfo(collectionNames []string) ([]*pb.CollectionInfo, error) {
	resp, err := util.EtcdGet(reader.etcdCli, reader.collectionPrefix()+"/", clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get all collection data", zap.Error(err))
		return nil, err
	}
	var existedCollectionInfos []*pb.CollectionInfo

	for _, kv := range resp.Kvs {
		info := &pb.CollectionInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			log.Warn("fail to unmarshal collection info", zap.String("key", util.ToString(kv.Key)), zap.Error(err))
			return existedCollectionInfos, err
		}
		if lo.Contains(collectionNames, info.Schema.Name) {
			err = reader.fillCollectionField(info)
			if err != nil {
				return existedCollectionInfos, err
			}
			existedCollectionInfos = append(existedCollectionInfos, info)
		}
	}
	return existedCollectionInfos, nil
}

func (reader *MilvusCollectionReader) fillCollectionField(info *pb.CollectionInfo) error {
	filedPrefix := reader.fieldPrefix()
	prefix := path.Join(filedPrefix, strconv.FormatInt(info.ID, 10)) + "/"
	resp, err := util.EtcdGet(reader.etcdCli, prefix, clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get the collection field data", zap.String("prefix", prefix), zap.Error(err))
		return err
	}
	if len(resp.Kvs) == 0 {
		err = errors.New("not found the collection field data")
		log.Warn(err.Error(), zap.String("prefix", filedPrefix))
		return err
	}
	var fields []*schemapb.FieldSchema
	for _, kv := range resp.Kvs {
		field := &schemapb.FieldSchema{}
		err = proto.Unmarshal(kv.Value, field)
		if err != nil {
			log.Warn("fail to unmarshal filed schema info",
				zap.String("key", util.ToString(kv.Key)), zap.Error(err))
			return err
		}
		if field.FieldID >= 100 {
			fields = append(fields, field)
		}
	}
	info.Schema.Fields = fields
	return nil
}

func (reader *MilvusCollectionReader) readStreamData(info *pb.CollectionInfo, sendCreateMsg bool) {
	isRepeatCollection := func(id int64) bool {
		reader.readingLock.Lock()
		defer reader.readingLock.Unlock()

		if lo.Contains(reader.readingSteamCollection, id) {
			return true
		}
		reader.readingSteamCollection = append(reader.readingSteamCollection, id)
		return false
	}
	if isRepeatCollection(info.ID) {
		return
	}
	reader.monitor.OnSuccessGetACollectionInfo(info.ID, reader.collectionName(info))

	if sendCreateMsg {
		baseMsg := msgstream.BaseMsg{
			HashValues: []uint32{0},
		}
		schemaByte, err := json.Marshal(info.Schema)
		if err != nil {
			log.Warn("fail to marshal the collection schema", zap.Error(err))
			reader.monitor.OnFailReadStream(info.ID, reader.collectionName(info), "unknown", err)
			return
		}
		createCollectionMsg := &msgstream.CreateCollectionMsg{
			BaseMsg: baseMsg,
			CreateCollectionRequest: msgpb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_CreateCollection,
				},
				CollectionName: reader.collectionName(info),
				CollectionID:   info.ID,
				Schema:         schemaByte,
			},
		}
		reader.sendData(&model.CDCData{
			Msg: createCollectionMsg,
			Extra: map[string]any{
				model.ShardNumKey:             info.ShardsNum,
				model.ConsistencyLevelKey:     info.ConsistencyLevel,
				model.CollectionPropertiesKey: info.Properties,
			},
		})
	}

	vchannels := info.VirtualChannelNames
	w := &sync.WaitGroup{}
	dropCollectionDataMap := &sync.Map{}
	w.Add(len(vchannels))
	for _, vchannel := range vchannels {
		position, err := reader.collectionPosition(info, vchannel)
		handleError := func() {
			reader.monitor.OnFailReadStream(info.ID, reader.collectionName(info), vchannel, err)
			w.Done()
			reader.isQuit.Store(true)
			dropCollectionDataMap = nil
		}
		if err != nil {
			log.Warn("fail to find the collection position", zap.String("vchannel", vchannel), zap.Error(err))
			handleError()
			return
		}
		stream, err := reader.msgStream()
		if err != nil {
			log.Warn("fail to new message stream", zap.String("vchannel", vchannel), zap.Error(err))
			handleError()
			return
		}
		msgChan, err := reader.msgStreamChan(vchannel, position, stream)
		if err != nil {
			stream.Close()
			log.Warn("fail to get message stream chan", zap.String("vchannel", vchannel), zap.Error(err))
			handleError()
			return
		}
		reader.closeStreamFuncs = append(reader.closeStreamFuncs, stream.Close)
		reader.goReadMsg(reader.collectionName(info), vchannel, msgChan, w, dropCollectionDataMap)
	}
	go func() {
		w.Wait()

		dropCollectionCdcData := &model.CDCData{
			Extra: make(map[string]any),
		}
		var otherDropData []*msgstream.DropCollectionMsg
		i := 0
		dropCollectionDataMap.Range(func(_, value any) bool {
			data := value.(*model.CDCData)
			if dropCollectionCdcData.Msg == nil {
				dropCollectionCdcData.Msg = data.Msg
			} else {
				otherDropData = append(otherDropData, data.Msg.(*msgstream.DropCollectionMsg))
			}
			i++
			return true
		})
		if i != len(vchannels) {
			return
		}

		dropCollectionCdcData.Extra[model.DropCollectionMsgsKey] = otherDropData
		reader.sendData(dropCollectionCdcData)
	}()
}

func (reader *MilvusCollectionReader) collectionPosition(info *pb.CollectionInfo, vchannelName string) (*msgstream.MsgPosition, error) {
	pchannel := util.ToPhysicalChannel(vchannelName)
	for _, collection := range reader.collections {
		if collection.collectionName == reader.collectionName(info) &&
			collection.positions != nil {
			if pair, ok := collection.positions[pchannel]; ok {
				return &msgstream.MsgPosition{
					ChannelName: vchannelName,
					MsgID:       pair.GetData(),
				}, nil
			}
		}
	}
	return util.GetChannelStartPosition(vchannelName, info.StartPositions)
}

func (reader *MilvusCollectionReader) msgStream() (msgstream.MsgStream, error) {
	var factory msgstream.Factory
	if reader.mqConfig.Pulsar.Address != "" {
		factory = reader.factoryCreator.NewPmsFactory(&reader.mqConfig.Pulsar)
	} else if reader.mqConfig.Kafka.Address != "" {
		factory = reader.factoryCreator.NewKmsFactory(&reader.mqConfig.Kafka)
	} else {
		return nil, errors.New("fail to get the msg stream, check the mqConfig param")
	}
	stream, err := factory.NewMsgStream(context.Background())
	if err != nil {
		log.Warn("fail to new the msg stream", zap.Error(err))
	}
	return stream, err
}

func (reader *MilvusCollectionReader) msgStreamChan(vchannel string, position *msgstream.MsgPosition, stream msgstream.MsgStream) (<-chan *msgstream.MsgPack, error) {
	consumeSubName := vchannel + string(rand.Int31())
	pchannelName := util.ToPhysicalChannel(vchannel)
	stream.AsConsumer([]string{pchannelName}, consumeSubName, mqwrapper.SubscriptionPositionUnknown)
	position.ChannelName = pchannelName
	err := stream.Seek([]*msgstream.MsgPosition{position})
	if err != nil {
		log.Warn("fail to seek the msg position", zap.String("vchannel", vchannel), zap.Error(err))
		return nil, err
	}

	return stream.Chan(), nil
}

func (reader *MilvusCollectionReader) goReadMsg(collectionName string, vchannelName string, c <-chan *msgstream.MsgPack,
	w *sync.WaitGroup, dropCollectionDataMap *sync.Map) {
	go func() {
		defer w.Done()
		for {
			empty := true
			dropCollectionDataMap.Range(func(_, _ any) bool {
				empty = false
				return false
			})
			if reader.isQuit.Load() && empty {
				return
			}
			msgPack := <-c
			if msgPack == nil {
				return
			}
			for _, msg := range msgPack.Msgs {
				msgType := msg.Type().String()
				if reader.filterMsgType(msgType) {
					continue
				}
				if reader.filterMsg(collectionName, msg) {
					continue
				}
				if _, ok := msg.(*msgstream.DropCollectionMsg); ok {
					if dropCollectionDataMap != nil {
						dropCollectionDataMap.Store(vchannelName, &model.CDCData{
							Msg: msg,
						})
					}
					return
				}
				reader.sendData(&model.CDCData{
					Msg: msg,
				})
			}
		}
	}()
}

func (reader *MilvusCollectionReader) filterMsgType(msgType string) bool {
	return msgType == "TimeTick"
}

func (reader *MilvusCollectionReader) filterMsg(collectionName string, msg msgstream.TsMsg) bool {
	if x, ok := msg.(interface{ GetCollectionName() string }); ok {
		notEqual := x.GetCollectionName() != collectionName
		if notEqual {
			// TODO fubang look it
			log.Warn("filter msg",
				zap.String("current_collection_name", collectionName),
				zap.String("msg_collection_name", x.GetCollectionName()),
				zap.Any("msg_type", msg.Type()))
			reader.monitor.OnFilterReadMsg(msg.Type().String())
		}
		return notEqual
	}
	return true
}

func (reader *MilvusCollectionReader) CancelWatchCollection() {
	if reader.cancelWatch != nil {
		reader.cancelWatch()
	}
}

func (reader *MilvusCollectionReader) QuitRead(ctx context.Context) {
	reader.quitOnce.Do(func() {
		reader.isQuit.Store(true)
		reader.CancelWatchCollection()
		for _, closeFunc := range reader.closeStreamFuncs {
			closeFunc()
		}
	})
}

func (reader *MilvusCollectionReader) sendData(data *model.CDCData) {
	reader.dataChan <- data
}
