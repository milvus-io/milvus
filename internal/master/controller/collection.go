package controller

import (
	"log"
	"strconv"
	"time"

	"github.com/czs007/suvlim/internal/conf"
	"github.com/czs007/suvlim/internal/master/collection"
	messagepb "github.com/czs007/suvlim/internal/proto/message"
	"github.com/czs007/suvlim/internal/master/id"
	"github.com/czs007/suvlim/internal/master/kv"
	"github.com/czs007/suvlim/internal/master/segment"
)

func CollectionController(ch chan *messagepb.Mapping, kvbase kv.Base, errch chan error) {
	for collectionMeta := range ch {
		sID := id.New().Uint64()
		cID := id.New().Uint64()
		s2ID := id.New().Uint64()
		fieldMetas := []*messagepb.FieldMeta{}
		if collectionMeta.Schema != nil {
			fieldMetas = collectionMeta.Schema.FieldMetas
		}
		c := collection.NewCollection(cID, collectionMeta.CollectionName,
			time.Now(), fieldMetas, []uint64{sID, s2ID},
			[]string{"default"})
		cm := collection.GrpcMarshal(&c)
		s := segment.NewSegment(sID, cID, collectionMeta.CollectionName, "default", 0, 511, time.Now(), time.Unix(1<<36-1, 0))
		s2 := segment.NewSegment(s2ID, cID, collectionMeta.CollectionName, "default", 512, 1023, time.Now(), time.Unix(1<<36-1, 0))
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

func WriteCollection2Datastore(collectionMeta *messagepb.Mapping, kvbase kv.Base) error {
	sID := id.New().Uint64()
	cID := id.New().Uint64()
	fieldMetas := []*messagepb.FieldMeta{}
	if collectionMeta.Schema != nil {
		fieldMetas = collectionMeta.Schema.FieldMetas
	}
	c := collection.NewCollection(cID, collectionMeta.CollectionName,
		time.Now(), fieldMetas, []uint64{sID},
		[]string{"default"})
	cm := collection.GrpcMarshal(&c)
	s := segment.NewSegment(sID, cID, collectionMeta.CollectionName, "default", 0, conf.Config.Pulsar.TopicNum, time.Now(), time.Unix(1<<46-1, 0))
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

func UpdateCollectionIndex(index *messagepb.IndexParam, kvbase kv.Base) error {
	collectionName := index.CollectionName
	collectionJSON, err := kvbase.Load("collection/" + collectionName)
	if err != nil {
		return err
	}
	c, err := collection.JSON2Collection(collectionJSON)
	if err != nil {
		return err
	}
	for k, v := range c.IndexParam {
		if v.IndexName == index.IndexName {
			c.IndexParam[k] = v
		}
	}
	c.IndexParam = append(c.IndexParam, index)
	cm := collection.GrpcMarshal(c)
	collectionData, err := collection.Collection2JSON(*cm)
	if err != nil {
		return err
	}
	err = kvbase.Save("collection/"+collectionName, collectionData)
	if err != nil {
		return err
	}
	return nil
}
