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
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/cdc/core/config"
	"github.com/milvus-io/milvus/cdc/core/model"
	"github.com/milvus-io/milvus/cdc/core/mq"
	"github.com/milvus-io/milvus/cdc/core/mq/api"
	"github.com/milvus-io/milvus/cdc/core/pb"
	"github.com/milvus-io/milvus/cdc/core/util"
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
	allCollectionNames []string
	dataChan           chan *model.CDCData
	cancelWatch        context.CancelFunc

	startOnce sync.Once
	quitOnce  sync.Once
	// only read it, CAN'T write it excluding in the QuitRead
	isQuit bool

	// Please no read or write it excluding in the beginning of readStreamData method
	readingSteamCollection []int64
	readingLock            sync.Mutex
}

func NewMilvusCollectionReader(options ...config.Option[*MilvusCollectionReader]) (*MilvusCollectionReader, error) {
	reader := &MilvusCollectionReader{
		monitor:     NewDefaultMonitor(),
		dataChanLen: 10,
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
		reader.goWatchCollection(watchCtx, wg)
		reader.goGetAllCollections(wg)
		reader.goCancelWatch(wg)
	})

	return reader.dataChan
}

func (reader *MilvusCollectionReader) goWatchCollection(watchCtx context.Context, wg *sync.WaitGroup) {
	go func() {
		// watch collection prefix to avoid new collection while getting the all collection
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
						log.Info("collection put", zap.Any("state", getCollectionState(info)), zap.String("key", collectionKey))
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
									// TODO fubang how to deal, delete collection a and then recreate collection a
									wg.Done()
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
}

func (reader *MilvusCollectionReader) goCancelWatch(wg *sync.WaitGroup) {
	go func() {
		wg.Wait()
		reader.cancelWatch()
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
	resp, err := util.EtcdGet(reader.etcdCli,
		path.Join(filedPrefix, strconv.FormatInt(info.ID, 10))+"/", clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get the collection field data", zap.String("prefix", filedPrefix), zap.Error(err))
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
		baseMsg := api.BaseMsg{
			HashValues: []uint32{0},
		}
		schemaByte, err := json.Marshal(info.Schema)
		if err != nil {
			log.Warn("fail to marshal the collection schema", zap.Error(err))
			reader.monitor.OnFailReadStream(info.ID, reader.collectionName(info), "unknown", err)
			return
		}
		createCollectionMsg := &api.CreateCollectionMsg{
			BaseMsg: baseMsg,
			CreateCollectionRequest: pb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_CreateCollection,
				},
				CollectionName: reader.collectionName(info),
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
	dropCollectionDataMap := make(map[string]*model.CDCData)
	w.Add(len(vchannels))
	for _, vchannel := range vchannels {
		position, err := reader.collectionPosition(info, vchannel)
		handleError := func() {
			reader.monitor.OnFailReadStream(info.ID, reader.collectionName(info), vchannel, err)
			w.Done()
			reader.isQuit = true
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
			log.Warn("fail to get message stream chan", zap.String("vchannel", vchannel), zap.Error(err))
			handleError()
			return
		}
		reader.goReadMsg(reader.collectionName(info), vchannel, msgChan, w, dropCollectionDataMap)
	}
	go func() {
		w.Wait()
		if len(dropCollectionDataMap) != len(vchannels) {
			return
		}
		dropCollectionCdcData := &model.CDCData{
			Extra: make(map[string]any),
		}
		var otherDropData []*api.DropCollectionMsg
		for _, data := range dropCollectionDataMap {
			if dropCollectionCdcData.Msg == nil {
				dropCollectionCdcData.Msg = data.Msg
			} else {
				otherDropData = append(otherDropData, data.Msg.(*api.DropCollectionMsg))
			}
		}

		dropCollectionCdcData.Extra[model.DropCollectionMsgsKey] = otherDropData
		reader.sendData(dropCollectionCdcData)
	}()
}

func (reader *MilvusCollectionReader) collectionPosition(info *pb.CollectionInfo, vchannelName string) (*pb.MsgPosition, error) {
	for _, collection := range reader.collections {
		if collection.collectionName == reader.collectionName(info) &&
			collection.positions != nil {
			if pair, ok := collection.positions[util.ToPhysicalChannel(vchannelName)]; ok {
				return &pb.MsgPosition{
					ChannelName: vchannelName,
					MsgID:       pair.GetData(),
				}, nil
			}
		}
	}
	return util.GetChannelStartPosition(vchannelName, info.StartPositions)
}

func (reader *MilvusCollectionReader) msgStream() (api.MsgStream, error) {
	var factory api.Factory
	if reader.mqConfig.Pulsar.Address != "" {
		factory = mq.NewPmsFactory(&reader.mqConfig.Pulsar)
	} else if reader.mqConfig.Kafka.Address != "" {
		factory = mq.NewKmsFactory(&reader.mqConfig.Kafka)
	} else {
		return nil, errors.New("fail to get the msg stream, check the mqConfig param")
	}
	stream, err := factory.NewMsgStream(context.Background())
	if err != nil {
		log.Warn("fail to new the msg stream", zap.Error(err))
	}
	return stream, err
}

func (reader *MilvusCollectionReader) msgStreamChan(vchannel string, position *pb.MsgPosition, stream api.MsgStream) (<-chan *api.MsgPack, error) {
	consumeSubName := vchannel + string(rand.Int31())

	pchannelName := util.ToPhysicalChannel(vchannel)
	stream.AsConsumer([]string{pchannelName}, consumeSubName, api.SubscriptionPositionUnknown)
	position.ChannelName = pchannelName
	err := stream.Seek([]*pb.MsgPosition{position})
	if err != nil {
		log.Warn("fail to seek the msg position", zap.String("vchannel", vchannel), zap.Error(err))
		return nil, err
	}

	return stream.Chan(), nil
}

func (reader *MilvusCollectionReader) goReadMsg(collectionName string, vchannelName string, c <-chan *api.MsgPack,
	w *sync.WaitGroup, dropCollectionDataMap map[string]*model.CDCData) {
	go func() {
		for {
			if reader.isQuit && len(dropCollectionDataMap) == 0 {
				w.Done()
				return
			}
			msgPack := <-c
			for _, msg := range msgPack.Msgs {
				msgType := msg.Type().String()
				if reader.filterMsgType(msgType) {
					continue
				}
				if reader.filterMsg(collectionName, msg) {
					continue
				}
				if _, ok := msg.(*api.DropCollectionMsg); ok {
					w.Done()
					if dropCollectionDataMap != nil {
						dropCollectionDataMap[vchannelName] = &model.CDCData{
							Msg: msg,
						}
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

func (reader *MilvusCollectionReader) filterMsg(collectionName string, msg api.TsMsg) bool {
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
		reader.isQuit = true
		reader.CancelWatchCollection()
	})
}

func (reader *MilvusCollectionReader) sendData(data *model.CDCData) {
	reader.dataChan <- data
}
