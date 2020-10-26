package controller

import (
	"log"
	"strconv"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/master/collection"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/master/id"
	"github.com/zilliztech/milvus-distributed/internal/master/kv"
	"github.com/zilliztech/milvus-distributed/internal/master/segment"
)

func CollectionController(ch chan *schemapb.CollectionSchema, kvbase kv.Base, errch chan error) {
	for collectionMeta := range ch {
		sID := id.New().Uint64()
		cID := id.New().Uint64()
		s2ID := id.New().Uint64()
		fieldMetas := []*schemapb.FieldSchema{}
		if collectionMeta.Fields != nil {
			fieldMetas = collectionMeta.Fields
		}
		c := collection.NewCollection(cID, collectionMeta.Name,
			time.Now(), fieldMetas, []uint64{sID, s2ID},
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
		err = kvbase.Save("collection/"+strconv.FormatUint(cID, 10), collectionData)
		if err != nil {
			log.Fatal(err)
		}
		err = kvbase.Save("segment/"+strconv.FormatUint(sID, 10), segmentData)
		if err != nil {
			log.Fatal(err)
		}
		err = kvbase.Save("segment/"+strconv.FormatUint(s2ID, 10), s2Data)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func WriteCollection2Datastore(collectionMeta *schemapb.CollectionSchema, kvbase kv.Base) error {
	sID := id.New().Uint64()
	cID := id.New().Uint64()
	fieldMetas := []*schemapb.FieldSchema{}
	if collectionMeta.Fields != nil {
		fieldMetas = collectionMeta.Fields
	}
	c := collection.NewCollection(cID, collectionMeta.Name,
		time.Now(), fieldMetas, []uint64{sID},
		[]string{"default"})
	cm := collection.GrpcMarshal(&c)
	s := segment.NewSegment(sID, cID, collectionMeta.Name, "default", 0, conf.Config.Pulsar.TopicNum, time.Now(), time.Unix(1<<46-1, 0))
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
	err = kvbase.Save("collection/"+strconv.FormatUint(cID, 10), collectionData)
	if err != nil {
		log.Fatal(err)
		return err
	}
	err = kvbase.Save("segment/"+strconv.FormatUint(sID, 10), segmentData)
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil

}
