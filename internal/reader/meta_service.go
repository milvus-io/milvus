package reader

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"log"
	"path"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

const (
	CollectionPrefix = "/collection/"
	SegmentPrefix    = "/segment/"
)

type metaService struct {
	ctx       context.Context
	kvBase    *kv.EtcdKV
	container *ColSegContainer
}

func newMetaService(ctx context.Context, container *ColSegContainer) *metaService {
	ETCDAddr := "http://"
	ETCDAddr += conf.Config.Etcd.Address
	ETCDPort := conf.Config.Etcd.Port
	ETCDAddr = ETCDAddr + ":" + strconv.FormatInt(int64(ETCDPort), 10)

	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{ETCDAddr},
		DialTimeout: 5 * time.Second,
	})

	return &metaService{
		ctx:       ctx,
		kvBase:    kv.NewEtcdKV(cli, conf.Config.Etcd.Rootpath),
		container: container,
	}
}

func (mService *metaService) start() {
	// init from meta
	err := mService.loadCollections()
	if err != nil {
		log.Fatal("metaService loadCollections failed")
	}
	err = mService.loadSegments()
	if err != nil {
		log.Fatal("metaService loadSegments failed")
	}

	metaChan := mService.kvBase.WatchWithPrefix("")
	for {
		select {
		case <-mService.ctx.Done():
			return
		case resp := <-metaChan:
			err := mService.processResp(resp)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func GetCollectionObjID(key string) string {
	prefix := path.Join(conf.Config.Etcd.Rootpath, CollectionPrefix) + "/"
	return strings.TrimPrefix(key, prefix)
}

func GetSegmentObjID(key string) string {
	prefix := path.Join(conf.Config.Etcd.Rootpath, SegmentPrefix) + "/"
	return strings.TrimPrefix(key, prefix)
}

func isCollectionObj(key string) bool {
	prefix := path.Join(conf.Config.Etcd.Rootpath, CollectionPrefix) + "/"
	prefix = strings.TrimSpace(prefix)
	index := strings.Index(key, prefix)

	return index == 0
}

func isSegmentObj(key string) bool {
	prefix := path.Join(conf.Config.Etcd.Rootpath, SegmentPrefix) + "/"
	prefix = strings.TrimSpace(prefix)
	index := strings.Index(key, prefix)

	return index == 0
}

func isSegmentChannelRangeInQueryNodeChannelRange(segment *etcdpb.SegmentMeta) bool {
	if segment.ChannelStart > segment.ChannelEnd {
		log.Printf("Illegal segment channel range")
		return false
	}

	var queryNodeChannelStart = conf.Config.Reader.TopicStart
	var queryNodeChannelEnd = conf.Config.Reader.TopicEnd

	if segment.ChannelStart >= int32(queryNodeChannelStart) && segment.ChannelEnd <= int32(queryNodeChannelEnd) {
		return true
	}

	return false
}

func printCollectionStruct(obj *etcdpb.CollectionMeta) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v)
	typeOfS := v.Type()

	for i := 0; i < v.NumField(); i++ {
		if typeOfS.Field(i).Name == "GrpcMarshalString" {
			continue
		}
		fmt.Printf("Field: %s\tValue: %v\n", typeOfS.Field(i).Name, v.Field(i).Interface())
	}
}

func printSegmentStruct(obj *etcdpb.SegmentMeta) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v)
	typeOfS := v.Type()

	for i := 0; i < v.NumField(); i++ {
		fmt.Printf("Field: %s\tValue: %v\n", typeOfS.Field(i).Name, v.Field(i).Interface())
	}
}

func (mService *metaService) processCollectionCreate(id string, value string) {
	println(fmt.Sprintf("Create Collection:$%s$", id))

	col := mService.collectionUnmarshal(value)
	if col != nil {
		newCollection := mService.container.addCollection(col, value)
		for _, partitionTag := range col.PartitionTags {
			err := mService.container.addPartition(newCollection, partitionTag)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func (mService *metaService) processSegmentCreate(id string, value string) {
	println("Create Segment: ", id)

	seg := mService.segmentUnmarshal(value)
	if !isSegmentChannelRangeInQueryNodeChannelRange(seg) {
		return
	}

	// TODO: what if seg == nil? We need to notify master and return rpc request failed
	if seg != nil {
		var col, err = mService.container.getCollectionByID(seg.CollectionId)
		if err != nil {
			log.Println(err)
			return
		}
		if col != nil {
			var partition, err = mService.container.getPartitionByTag(seg.PartitionTag)
			if err != nil {
				log.Println(err)
				return
			}
			if partition != nil {
				err = mService.container.addSegment(col, partition, seg.SegmentId)
				if err != nil {
					log.Println(err)
					return
				}
			}
		}
	}
}

func (mService *metaService) processCreate(key string, msg string) {
	println("process create", key)
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

func (mService *metaService) processSegmentModify(id string, value string) {
	seg := mService.segmentUnmarshal(value)

	if !isSegmentChannelRangeInQueryNodeChannelRange(seg) {
		return
	}

	if seg != nil {
		targetSegment, err := mService.container.getSegmentByID(seg.SegmentId)
		if err != nil {
			log.Println(err)
			return
		}

		// TODO: do modify
		fmt.Println(targetSegment)
	}
}

func (mService *metaService) processCollectionModify(id string, value string) {
	println("Modify Collection: ", id)
}

func (mService *metaService) processModify(key string, msg string) {
	if isCollectionObj(key) {
		objID := GetCollectionObjID(key)
		mService.processCollectionModify(objID, msg)
	} else if isSegmentObj(key) {
		objID := GetSegmentObjID(key)
		mService.processSegmentModify(objID, msg)
	} else {
		println("can not process modify msg:", key)
	}
}

func (mService *metaService) processSegmentDelete(id string) {
	println("Delete segment: ", id)

	var segmentID, err = strconv.ParseInt(id, 10, 64)
	if err != nil {
		log.Println("Cannot parse segment id:" + id)
	}

	seg, err := mService.container.getSegmentByID(segmentID)
	if err != nil {
		log.Println(err)
		return
	}

	err = mService.container.removeSegment(seg)
	if err != nil {
		log.Println(err)
		return
	}
}

func (mService *metaService) processCollectionDelete(id string) {
	println("Delete collection: ", id)

	var collectionID, err = strconv.ParseInt(id, 10, 64)
	if err != nil {
		log.Println("Cannot parse collection id:" + id)
	}

	targetCollection, err := mService.container.getCollectionByID(collectionID)
	if err != nil {
		log.Println(err)
		return
	}

	err = mService.container.removeCollection(targetCollection)
	if err != nil {
		log.Println(err)
		return
	}
}

func (mService *metaService) processDelete(key string) {
	println("process delete")

	if isCollectionObj(key) {
		objID := GetCollectionObjID(key)
		mService.processCollectionDelete(objID)
	} else if isSegmentObj(key) {
		objID := GetSegmentObjID(key)
		mService.processSegmentDelete(objID)
	} else {
		println("can not process delete msg:", key)
	}
}

func (mService *metaService) processResp(resp clientv3.WatchResponse) error {
	err := resp.Err()
	if err != nil {
		return err
	}

	for _, ev := range resp.Events {
		if ev.IsCreate() {
			key := string(ev.Kv.Key)
			msg := string(ev.Kv.Value)
			mService.processCreate(key, msg)
		} else if ev.IsModify() {
			key := string(ev.Kv.Key)
			msg := string(ev.Kv.Value)
			mService.processModify(key, msg)
		} else if ev.Type == mvccpb.DELETE {
			key := string(ev.Kv.Key)
			mService.processDelete(key)
		} else {
			println("Unrecognized etcd msg!")
		}
	}
	return nil
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
func (mService *metaService) collectionUnmarshal(value string) *etcdpb.CollectionMeta {
	col := etcdpb.CollectionMeta{}
	err := proto.Unmarshal([]byte(value), &col)
	if err != nil {
		log.Println(err)
		return nil
	}
	return &col
}

func (mService *metaService) collectionMarshal(col *etcdpb.CollectionMeta) string {
	value, err := proto.Marshal(col)
	if err != nil {
		log.Println(err)
		return ""
	}
	return string(value)
}

func (mService *metaService) segmentUnmarshal(value string) *etcdpb.SegmentMeta {
	seg := etcdpb.SegmentMeta{}
	err := proto.Unmarshal([]byte(value), &seg)
	if err != nil {
		log.Println(err)
		return nil
	}
	return &seg
}

func (mService *metaService) segmentMarshal(seg *etcdpb.SegmentMeta) string {
	value, err := proto.Marshal(seg)
	if err != nil {
		log.Println(err)
		return ""
	}
	return string(value)
}
