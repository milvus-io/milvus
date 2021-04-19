package controller

import (
	"log"
	"strconv"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/master/collection"
	"github.com/zilliztech/milvus-distributed/internal/master/id"
	"github.com/zilliztech/milvus-distributed/internal/master/segment"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID

func CollectionController(ch chan *schemapb.CollectionSchema, kvbase *kv.EtcdKV, errch chan error) {
	for collectionMeta := range ch {
		sID, _ := id.AllocOne()
		cID, _ := id.AllocOne()
		s2ID, _ := id.AllocOne()
		fieldMetas := []*schemapb.FieldSchema{}
		if collectionMeta.Fields != nil {
			fieldMetas = collectionMeta.Fields
		}
		c := collection.NewCollection(cID, collectionMeta.Name,
			time.Now(), fieldMetas, []UniqueID{sID, s2ID},
			[]string{"default"})
		cm := collection.GrpcMarshal(&c)
		s := segment.NewSegment(sID, cID, collectionMeta.Name, "default", 0, 511, time.Now(), time.Unix(1<<36-1, 0))
		s2 := segment.NewSegment(s2ID, cID, collectionMeta.Name, "default", 512, 1023, time.Now(), time.Unix(1<<36-1, 0))
		collectionData, _ := collection.Collection2JSON(*cm)
		segmentData, err := segment.Segment2JSON(s)
		if err != nil {
			log.Fatal(err)
		}
		s2Data, err := segment.Segment2JSON(s2)
		if err != nil {
			log.Fatal(err)
		}
		err = kvbase.Save("collection/"+strconv.FormatInt(cID, 10), collectionData)
		if err != nil {
			log.Fatal(err)
		}
		err = kvbase.Save("segment/"+strconv.FormatInt(sID, 10), segmentData)
		if err != nil {
			log.Fatal(err)
		}
		err = kvbase.Save("segment/"+strconv.FormatInt(s2ID, 10), s2Data)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func WriteCollection2Datastore(collectionMeta *schemapb.CollectionSchema, kvbase *kv.EtcdKV) error {
	sID, _ := id.AllocOne()
	cID, _ := id.AllocOne()
	fieldMetas := []*schemapb.FieldSchema{}
	if collectionMeta.Fields != nil {
		fieldMetas = collectionMeta.Fields
	}
	c := collection.NewCollection(cID, collectionMeta.Name,
		time.Now(), fieldMetas, []UniqueID{sID},
		[]string{"default"})
	cm := collection.GrpcMarshal(&c)
	pulsarTopicNum, err := gparams.GParams.Load("pulsar.topicnum")
	if err != nil {
		panic(err)
	}
	topicNum, err := strconv.Atoi(pulsarTopicNum)
	if err != nil {
		panic(err)
	}
	s := segment.NewSegment(sID, cID, collectionMeta.Name, "default", 0, topicNum, time.Now(), time.Unix(1<<46-1, 0))
	collectionData, err := collection.Collection2JSON(*cm)
	if err != nil {
		log.Fatal(err)
		return err
	}
	segmentData, err := segment.Segment2JSON(s)
	if err != nil {
		log.Fatal(err)
		return err
	}
	err = kvbase.Save("collection/"+strconv.FormatInt(cID, 10), collectionData)
	if err != nil {
		log.Fatal(err)
		return err
	}
	err = kvbase.Save("segment/"+strconv.FormatInt(sID, 10), segmentData)
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil

}
