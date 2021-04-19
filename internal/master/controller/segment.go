package controller

import (
	"fmt"
	"strconv"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/master/id"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/master/collection"
	"github.com/zilliztech/milvus-distributed/internal/master/segment"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

func ComputeCloseTime(ss internalpb.SegmentStatistics, kvbase *kv.EtcdKV) error {
	if int(ss.MemorySize) > int(conf.Config.Master.SegmentThreshole*0.8) {
		currentTime := time.Now()
		//memRate := int(ss.MemoryRate)
		memRate := 1 // to do
		if memRate == 0 {
			memRate = 1
		}
		sec := int(conf.Config.Master.SegmentThreshole*0.2) / memRate
		data, err := kvbase.Load("segment/" + strconv.Itoa(int(ss.SegmentId)))
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
		kvbase.Save("segment/"+strconv.Itoa(int(ss.SegmentId)), updateData)
		//create new segment
		newSegID, _ := id.AllocOne()
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
