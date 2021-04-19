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

package querynode

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
)

const (
	CollectionPrefix = "/collection/"
	SegmentPrefix    = "/segment/"
)

type metaService struct {
	ctx     context.Context
	kvBase  *etcdkv.EtcdKV
	replica ReplicaInterface
}

func newMetaService(ctx context.Context, replica ReplicaInterface) *metaService {
	ETCDAddr := Params.ETCDAddress
	MetaRootPath := Params.MetaRootPath
	var cli *clientv3.Client
	var err error

	connectEtcdFn := func() error {
		cli, err = clientv3.New(clientv3.Config{
			Endpoints:   []string{ETCDAddr},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return err
		}
		return nil
	}
	err = retry.Retry(100000, time.Millisecond*200, connectEtcdFn)
	if err != nil {
		panic(err)
	}

	return &metaService{
		ctx:     ctx,
		kvBase:  etcdkv.NewEtcdKV(cli, MetaRootPath),
		replica: replica,
	}
}

func (mService *metaService) start() {
	// init from meta
	err := mService.loadCollections()
	if err != nil {
		log.Error("metaService loadCollections failed")
	}
	err = mService.loadSegments()
	if err != nil {
		log.Error("metaService loadSegments failed")
	}
}

func GetCollectionObjID(key string) string {
	ETCDRootPath := Params.MetaRootPath

	prefix := path.Join(ETCDRootPath, CollectionPrefix) + "/"
	return strings.TrimPrefix(key, prefix)
}

func GetSegmentObjID(key string) string {
	ETCDRootPath := Params.MetaRootPath

	prefix := path.Join(ETCDRootPath, SegmentPrefix) + "/"
	return strings.TrimPrefix(key, prefix)
}

func isCollectionObj(key string) bool {
	ETCDRootPath := Params.MetaRootPath

	prefix := path.Join(ETCDRootPath, CollectionPrefix) + "/"
	prefix = strings.TrimSpace(prefix)
	index := strings.Index(key, prefix)

	return index == 0
}

func isSegmentObj(key string) bool {
	ETCDRootPath := Params.MetaRootPath

	prefix := path.Join(ETCDRootPath, SegmentPrefix) + "/"
	prefix = strings.TrimSpace(prefix)
	index := strings.Index(key, prefix)

	return index == 0
}

func printCollectionStruct(obj *etcdpb.CollectionInfo) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v)
	typeOfS := v.Type()

	for i := 0; i < v.NumField(); i++ {
		if typeOfS.Field(i).Name == "GrpcMarshalString" {
			continue
		}
		log.Debug("Field", zap.String("field name", typeOfS.Field(i).Name), zap.String("field", fmt.Sprintln(v.Field(i).Interface())))
	}
}

func printSegmentStruct(obj *datapb.SegmentInfo) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v)
	typeOfS := v.Type()

	for i := 0; i < v.NumField(); i++ {
		log.Debug("Field", zap.String("field name", typeOfS.Field(i).Name), zap.String("field", fmt.Sprintln(v.Field(i).Interface())))
	}
}

func (mService *metaService) processCollectionCreate(id string, value string) {
	col := mService.collectionUnmarshal(value)
	if col != nil {
		schema := col.Schema
		err := mService.replica.addCollection(col.ID, schema)
		if err != nil {
			log.Error(err.Error())
		}
		for _, partitionID := range col.PartitionIDs {
			err = mService.replica.addPartition(col.ID, partitionID)
			if err != nil {
				log.Error(err.Error())
			}
		}
	}
}

func (mService *metaService) processSegmentCreate(id string, value string) {
	//println("Create Segment: ", id)

	seg := mService.segmentUnmarshal(value)

	// TODO: what if seg == nil? We need to notify master and return rpc request failed
	if seg != nil {
		// TODO: get partition id from segment meta
		err := mService.replica.addSegment(seg.ID, seg.PartitionID, seg.CollectionID, segmentTypeGrowing)
		if err != nil {
			log.Error(err.Error())
			return
		}
	}
}

func (mService *metaService) processCreate(key string, msg string) {
	//println("process create", key)
	if isCollectionObj(key) {
		objID := GetCollectionObjID(key)
		mService.processCollectionCreate(objID, msg)
	} else if isSegmentObj(key) {
		objID := GetSegmentObjID(key)
		mService.processSegmentCreate(objID, msg)
	} else {
		println("can not process create msg:", key)
	}
}

func (mService *metaService) loadCollections() error {
	keys, values, err := mService.kvBase.LoadWithPrefix(CollectionPrefix)
	if err != nil {
		return err
	}

	for i := range keys {
		objID := GetCollectionObjID(keys[i])
		mService.processCollectionCreate(objID, values[i])
	}

	return nil
}

func (mService *metaService) loadSegments() error {
	keys, values, err := mService.kvBase.LoadWithPrefix(SegmentPrefix)
	if err != nil {
		return err
	}

	for i := range keys {
		objID := GetSegmentObjID(keys[i])
		mService.processSegmentCreate(objID, values[i])
	}

	return nil
}

//----------------------------------------------------------------------- Unmarshal and Marshal
func (mService *metaService) collectionUnmarshal(value string) *etcdpb.CollectionInfo {
	col := etcdpb.CollectionInfo{}
	err := proto.UnmarshalText(value, &col)
	if err != nil {
		log.Error(fmt.Errorf("QueryNode metaService UnmarshalText etcdpb.CollectionInfo err:%w", err).Error())
		return nil
	}
	return &col
}

func (mService *metaService) collectionMarshal(col *etcdpb.CollectionInfo) string {
	value := proto.MarshalTextString(col)
	if value == "" {
		log.Error("marshal collection failed")
		return ""
	}
	return value
}

func (mService *metaService) segmentUnmarshal(value string) *datapb.SegmentInfo {
	seg := datapb.SegmentInfo{}
	err := proto.UnmarshalText(value, &seg)
	if err != nil {
		log.Error(fmt.Errorf("QueryNode metaService UnmarshalText datapb.SegmentInfo err:%w", err).Error())
		return nil
	}
	return &seg
}

func (mService *metaService) segmentMarshal(seg *etcdpb.SegmentMeta) string {
	value := proto.MarshalTextString(seg)
	if value == "" {
		log.Error("marshal segment failed")
		return ""
	}
	return value
}
