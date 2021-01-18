package querynode

import (
	"context"
	"fmt"
	"log"
	"path"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
)

const (
	CollectionPrefix = "/collection/"
	SegmentPrefix    = "/segment/"
)

type metaService struct {
	ctx     context.Context
	kvBase  *etcdkv.EtcdKV
	replica collectionReplica
}

func newMetaService(ctx context.Context, replica collectionReplica) *metaService {
	ETCDAddr := Params.ETCDAddress
	MetaRootPath := Params.MetaRootPath

	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{ETCDAddr},
		DialTimeout: 5 * time.Second,
	})

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
		log.Fatal("metaService loadCollections failed")
	}
	err = mService.loadSegments()
	if err != nil {
		log.Fatal("metaService loadSegments failed")
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

func isSegmentChannelRangeInQueryNodeChannelRange(segment *etcdpb.SegmentMeta) bool {
	if segment.ChannelStart > segment.ChannelEnd {
		log.Printf("Illegal segment channel range")
		return false
	}

	var queryNodeChannelStart = Params.InsertChannelRange[0]
	var queryNodeChannelEnd = Params.InsertChannelRange[1]

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
	//println(fmt.Sprintf("Create Collection:$%s$", id))

	col := mService.collectionUnmarshal(value)
	if col != nil {
		schema := col.Schema
		err := mService.replica.addCollection(col.ID, schema)
		if err != nil {
			log.Println(err)
		}
		for _, partitionTag := range col.PartitionTags {
			err = mService.replica.addPartition(col.ID, partitionTag)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func (mService *metaService) processSegmentCreate(id string, value string) {
	//println("Create Segment: ", id)

	seg := mService.segmentUnmarshal(value)
	if !isSegmentChannelRangeInQueryNodeChannelRange(seg) {
		log.Println("Illegal segment channel range")
		return
	}

	// TODO: what if seg == nil? We need to notify master and return rpc request failed
	if seg != nil {
		err := mService.replica.addSegment(seg.SegmentID, seg.PartitionTag, seg.CollectionID)
		if err != nil {
			log.Println(err)
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

func (mService *metaService) processSegmentModify(id string, value string) {
	seg := mService.segmentUnmarshal(value)

	if !isSegmentChannelRangeInQueryNodeChannelRange(seg) {
		return
	}

	if seg != nil {
		targetSegment, err := mService.replica.getSegmentByID(seg.SegmentID)
		if err != nil {
			log.Println(err)
			return
		}

		// TODO: do modify
		fmt.Println(targetSegment)
	}
}

func (mService *metaService) processCollectionModify(id string, value string) {
	//println("Modify Collection: ", id)

	col := mService.collectionUnmarshal(value)
	if col != nil {
		err := mService.replica.addPartitionsByCollectionMeta(col)
		if err != nil {
			log.Println(err)
		}
		err = mService.replica.removePartitionsByCollectionMeta(col)
		if err != nil {
			log.Println(err)
		}
	}
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
	//println("Delete segment: ", id)

	var segmentID, err = strconv.ParseInt(id, 10, 64)
	if err != nil {
		log.Println("Cannot parse segment id:" + id)
	}

	err = mService.replica.removeSegment(segmentID)
	if err != nil {
		log.Println(err)
		return
	}
}

func (mService *metaService) processCollectionDelete(id string) {
	//println("Delete collection: ", id)

	var collectionID, err = strconv.ParseInt(id, 10, 64)
	if err != nil {
		log.Println("Cannot parse collection id:" + id)
	}

	err = mService.replica.removeCollection(collectionID)
	if err != nil {
		log.Println(err)
		return
	}
}

func (mService *metaService) processDelete(key string) {
	//println("process delete")

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
	err := proto.UnmarshalText(value, &col)
	if err != nil {
		log.Println(err)
		return nil
	}
	return &col
}

func (mService *metaService) collectionMarshal(col *etcdpb.CollectionMeta) string {
	value := proto.MarshalTextString(col)
	if value == "" {
		log.Println("marshal collection failed")
		return ""
	}
	return value
}

func (mService *metaService) segmentUnmarshal(value string) *etcdpb.SegmentMeta {
	seg := etcdpb.SegmentMeta{}
	err := proto.UnmarshalText(value, &seg)
	if err != nil {
		log.Println(err)
		return nil
	}
	return &seg
}

func (mService *metaService) segmentMarshal(seg *etcdpb.SegmentMeta) string {
	value := proto.MarshalTextString(seg)
	if value == "" {
		log.Println("marshal segment failed")
		return ""
	}
	return value
}
