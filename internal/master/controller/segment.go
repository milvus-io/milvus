package controller

import (
	"fmt"
	"strconv"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/master/collection"
	"github.com/zilliztech/milvus-distributed/internal/master/id"
	"github.com/zilliztech/milvus-distributed/internal/master/informer"
	"github.com/zilliztech/milvus-distributed/internal/master/kv"
	"github.com/zilliztech/milvus-distributed/internal/master/segment"
)

func SegmentStatsController(kvbase kv.Base, errch chan error) {
	ssChan := make(chan segment.SegmentStats, 10)
	ssClient := informer.NewPulsarClient()
	go segment.Listener(ssChan, ssClient)
	for {
		select {
		case ss := <-ssChan:
			errch <- ComputeCloseTime(ss, kvbase)
			errch <- UpdateSegmentStatus(ss, kvbase)
		case <-time.After(5 * time.Second):
			fmt.Println("wait for new request")
			return
		}
	}

}

func ComputeCloseTime(ss segment.SegmentStats, kvbase kv.Base) error {
	if int(ss.MemorySize) > int(conf.Config.Master.SegmentThreshole*0.8) {
		currentTime := time.Now()
		memRate := int(ss.MemoryRate)
		if memRate == 0 {
			memRate = 1
		}
		sec := int(conf.Config.Master.SegmentThreshole*0.2) / memRate
		data, err := kvbase.Load("segment/" + strconv.Itoa(int(ss.SegementID)))
		if err != nil {
			return err
		}
		seg, err := segment.JSON2Segment(data)
		if err != nil {
			return err
		}
		seg.CloseTimeStamp = uint64(currentTime.Add(time.Duration(sec) * time.Second).Unix())
		fmt.Println(seg)
		updateData, err := segment.Segment2JSON(*seg)
		if err != nil {
			return err
		}
		kvbase.Save("segment/"+strconv.Itoa(int(ss.SegementID)), updateData)
		//create new segment
		newSegID := id.New().Uint64()
		newSeg := segment.NewSegment(newSegID, seg.CollectionID, seg.CollectionName, "default", seg.ChannelStart, seg.ChannelEnd, currentTime, time.Unix(1<<36-1, 0))
		newSegData, err := segment.Segment2JSON(*&newSeg)
		if err != nil {
			return err
		}
		//save to kv store
		kvbase.Save("segment/"+strconv.Itoa(int(newSegID)), newSegData)
		// update collection data
		c, _ := kvbase.Load("collection/" + strconv.Itoa(int(seg.CollectionID)))
		collectionMeta, err := collection.JSON2Collection(c)
		if err != nil {
			return err
		}
		segIDs := collectionMeta.SegmentIDs
		segIDs = append(segIDs, newSegID)
		collectionMeta.SegmentIDs = segIDs
		cData, err := collection.Collection2JSON(*collectionMeta)
		if err != nil {
			return err
		}
		kvbase.Save("segment/"+strconv.Itoa(int(seg.CollectionID)), cData)
	}
	return nil
}

func UpdateSegmentStatus(ss segment.SegmentStats, kvbase kv.Base) error {
	segmentData, err := kvbase.Load("segment/" + strconv.Itoa(int(ss.SegementID)))
	if err != nil {
		return err
	}
	seg, err := segment.JSON2Segment(segmentData)
	if err != nil {
		return err
	}
	var changed bool
	changed = false
	if seg.Status != ss.Status {
		changed = true
		seg.Status = ss.Status
	}
	if seg.Rows != ss.Rows {
		changed = true
		seg.Rows = ss.Rows
	}

	if changed {
		segData, err := segment.Segment2JSON(*seg)
		if err != nil {
			return err
		}
		err = kvbase.Save("segment/"+strconv.Itoa(int(seg.CollectionID)), segData)
		if err != nil {
			return err
		}
	}
	return nil
}
