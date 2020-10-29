package reader

import (
	"fmt"
	"log"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/master/collection"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/master/segment"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

const (
	CollectionPrefix = "/collection/"
	SegmentPrefix    = "/segment/"
)

func GetCollectionObjId(key string) string {
	prefix := path.Join(conf.Config.Etcd.Rootpath, CollectionPrefix) + "/"
	return strings.TrimPrefix(key, prefix)
}

func GetSegmentObjId(key string) string {
	prefix := path.Join(conf.Config.Etcd.Rootpath, SegmentPrefix) + "/"
	return strings.TrimPrefix(key, prefix)
}

func isCollectionObj(key string) bool {
	prefix := path.Join(conf.Config.Etcd.Rootpath, CollectionPrefix) + "/"
	prefix = strings.TrimSpace(prefix)
	// println("prefix is :$", prefix)
	index := strings.Index(key, prefix)
	// println("index is :", index)
	return index == 0
}

func isSegmentObj(key string) bool {
	prefix := path.Join(conf.Config.Etcd.Rootpath, SegmentPrefix) + "/"
	prefix = strings.TrimSpace(prefix)
	index := strings.Index(key, prefix)
	return index == 0
}

func isSegmentChannelRangeInQueryNodeChannelRange(segment *segment.Segment) bool {
	if segment.ChannelStart > segment.ChannelEnd {
		log.Printf("Illegal segment channel range")
		return false
	}

	var queryNodeChannelStart = conf.Config.Reader.TopicStart
	var queryNodeChannelEnd = conf.Config.Reader.TopicEnd

	if segment.ChannelStart >= queryNodeChannelStart && segment.ChannelEnd <= queryNodeChannelEnd {
		return true
	}

	return false
}

func printCollectionStruct(obj *collection.Collection) {
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

func printSegmentStruct(obj *segment.Segment) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v)
	typeOfS := v.Type()

	for i := 0; i < v.NumField(); i++ {
		fmt.Printf("Field: %s\tValue: %v\n", typeOfS.Field(i).Name, v.Field(i).Interface())
	}
}

func (node *QueryNode) processCollectionCreate(id string, value string) {
	println(fmt.Sprintf("Create Collection:$%s$", id))
	collection, err := collection.JSON2Collection(value)
	if err != nil {
		println("error of json 2 collection")
		println(err.Error())
	}
	//printCollectionStruct(collection)
	newCollection := node.NewCollection(collection.ID, collection.Name, collection.GrpcMarshalString)
	for _, partitionTag := range collection.PartitionTags {
		newCollection.NewPartition(partitionTag)
	}
}

func (node *QueryNode) processSegmentCreate(id string, value string) {
	println("Create Segment: ", id)
	segment, err := segment.JSON2Segment(value)
	if err != nil {
		println("error of json 2 segment")
		println(err.Error())
	}
	//printSegmentStruct(segment)

	if !isSegmentChannelRangeInQueryNodeChannelRange(segment) {
		return
	}

	collection := node.GetCollectionByID(segment.CollectionID)
	if collection != nil {
		partition := collection.GetPartitionByName(segment.PartitionTag)
		if partition != nil {
			newSegmentID := int64(segment.SegmentID) // todo change all to uint64
			// start new segment and add it into partition.OpenedSegments
			newSegment := partition.NewSegment(newSegmentID)
			// newSegment.SegmentStatus = SegmentOpened
			newSegment.SegmentCloseTime = segment.CloseTimeStamp
			node.SegmentsMap[newSegmentID] = newSegment
		}
	}
	// segment.CollectionName
}

func (node *QueryNode) processCreate(key string, msg string) {
	println("process create", key)
	if isCollectionObj(key) {
		objID := GetCollectionObjId(key)
		node.processCollectionCreate(objID, msg)
	} else if isSegmentObj(key) {
		objID := GetSegmentObjId(key)
		node.processSegmentCreate(objID, msg)
	} else {
		println("can not process create msg:", key)
	}
}

func (node *QueryNode) processSegmentModify(id string, value string) {
	// println("Modify Segment: ", id)

	segment, err := segment.JSON2Segment(value)
	if err != nil {
		println("error of json 2 segment")
		println(err.Error())
	}
	// printSegmentStruct(segment)

	if !isSegmentChannelRangeInQueryNodeChannelRange(segment) {
		return
	}

	seg, err := node.GetSegmentBySegmentID(int64(segment.SegmentID)) // todo change  to uint64
	if seg != nil {
		seg.SegmentCloseTime = segment.CloseTimeStamp
	}
}

func (node *QueryNode) processCollectionModify(id string, value string) {
	// println("Modify Collection: ", id)
	collection, err := collection.JSON2Collection(value)
	if err != nil {
		println("error of json 2 collection")
		println(err.Error())
	}
	// printCollectionStruct(collection)

	goCollection := node.GetCollectionByID(collection.ID)
	if goCollection != nil {
		node.UpdateIndexes(goCollection, &collection.GrpcMarshalString)
	}

}

func (node *QueryNode) processModify(key string, msg string) {
	// println("process modify")
	if isCollectionObj(key) {
		objID := GetCollectionObjId(key)
		node.processCollectionModify(objID, msg)
	} else if isSegmentObj(key) {
		objID := GetSegmentObjId(key)
		node.processSegmentModify(objID, msg)
	} else {
		println("can not process modify msg:", key)
	}
}

func (node *QueryNode) processSegmentDelete(id string) {
	println("Delete segment: ", id)

	segmentId, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		log.Println("Cannot parse segment id:" + id)
	}

	for _, col := range node.Collections {
		for _, p := range col.Partitions {
			for _, s := range p.Segments {
				if s.SegmentId == segmentId {
					p.DeleteSegment(node, s)
				}
			}
		}
	}
}

func (node *QueryNode) processCollectionDelete(id string) {
	println("Delete collection: ", id)

	collectionId, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		log.Println("Cannot parse collection id:" + id)
	}

	targetCollection := node.GetCollectionByID(collectionId)
	node.DeleteCollection(targetCollection)
}

func (node *QueryNode) processDelete(key string) {
	println("process delete")
	if isCollectionObj(key) {
		objID := GetCollectionObjId(key)
		node.processCollectionDelete(objID)
	} else if isSegmentObj(key) {
		objID := GetSegmentObjId(key)
		node.processSegmentDelete(objID)
	} else {
		println("can not process delete msg:", key)
	}
}

func (node *QueryNode) processResp(resp clientv3.WatchResponse) error {
	err := resp.Err()
	if err != nil {
		return err
	}
	// println("processResp!!!!!\n")

	for _, ev := range resp.Events {
		if ev.IsCreate() {
			key := string(ev.Kv.Key)
			msg := string(ev.Kv.Value)
			node.processCreate(key, msg)
		} else if ev.IsModify() {
			key := string(ev.Kv.Key)
			msg := string(ev.Kv.Value)
			node.processModify(key, msg)
		} else if ev.Type == mvccpb.DELETE {
			key := string(ev.Kv.Key)
			node.processDelete(key)
		} else {
			println("Unrecognized etcd msg!")
		}
	}
	return nil
}

func (node *QueryNode) loadCollections() error {
	keys, values := node.kvBase.LoadWithPrefix(CollectionPrefix)
	for i := range keys {
		objID := GetCollectionObjId(keys[i])
		node.processCollectionCreate(objID, values[i])
	}
	return nil
}
func (node *QueryNode) loadSegments() error {
	keys, values := node.kvBase.LoadWithPrefix(SegmentPrefix)
	for i := range keys {
		objID := GetSegmentObjId(keys[i])
		node.processSegmentCreate(objID, values[i])
	}
	return nil
}

func (node *QueryNode) InitFromMeta() error {
	//pass
	etcdAddr := "http://"
	etcdAddr += conf.Config.Etcd.Address
	etcdPort := conf.Config.Etcd.Port
	etcdAddr = etcdAddr + ":" + strconv.FormatInt(int64(etcdPort), 10)
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	//defer cli.Close()
	node.kvBase = kv.NewEtcdKV(cli, conf.Config.Etcd.Rootpath)
	node.loadCollections()
	node.loadSegments()
	return nil
}

func (node *QueryNode) RunMetaService(wg *sync.WaitGroup) {
	//node.InitFromMeta()
	metaChan := node.kvBase.WatchWithPrefix("")
	for {
		select {
		case <-node.ctx.Done():
			wg.Done()
			println("DONE!!!!!!")
			return
		case resp := <-metaChan:
			node.processResp(resp)
		}
	}
}
