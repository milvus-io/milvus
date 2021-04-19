package reader

import (
	"fmt"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"log"
	"strconv"
	"time"
)

//func (node *QueryNode) SegmentsManagement() {
//	//node.queryNodeTimeSync.UpdateTSOTimeSync()
//	//var timeNow = node.queryNodeTimeSync.TSOTimeSync
//
//	timeNow := node.messageClient.GetTimeNow() >> 18
//
//	for _, collection := range node.Collections {
//		for _, partition := range collection.Partitions {
//			for _, segment := range partition.Segments {
//				if segment.SegmentStatus != SegmentOpened {
//					continue
//				}
//
//				// fmt.Println("timeNow = ", timeNow, "SegmentCloseTime = ", segment.SegmentCloseTime)
//				if timeNow >= segment.SegmentCloseTime {
//					go segment.CloseSegment(collection)
//				}
//			}
//		}
//	}
//}

//func (node *QueryNode) SegmentManagementService() {
//	sleepMillisecondTime := 1000
//	fmt.Println("do segments management in ", strconv.Itoa(sleepMillisecondTime), "ms")
//	for {
//		select {
//		case <-node.ctx.Done():
//			return
//		default:
//			time.Sleep(time.Duration(sleepMillisecondTime) * time.Millisecond)
//			node.SegmentsManagement()
//		}
//	}
//}

func (node *QueryNode) SegmentStatistic(sleepMillisecondTime int) {
	var statisticData = make([]internalpb.SegmentStatistics, 0)

	for segmentID, segment := range node.SegmentsMap {
		currentMemSize := segment.GetMemSize()
		segment.LastMemSize = currentMemSize

		segmentNumOfRows := segment.GetRowCount()

		stat := internalpb.SegmentStatistics{
			// TODO: set master pb's segment id type from uint64 to int64
			SegmentId:  segmentID,
			MemorySize: currentMemSize,
			NumRows: segmentNumOfRows,
		}

		statisticData = append(statisticData, stat)
	}

	// fmt.Println("Publish segment statistic")
	// fmt.Println(statisticData)
	var status = node.PublicStatistic(&statisticData)
	if status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		log.Printf("Publish segments statistic failed")
	}
}

func (node *QueryNode) SegmentStatisticService() {
	sleepMillisecondTime := 1000
	fmt.Println("do segments statistic in ", strconv.Itoa(sleepMillisecondTime), "ms")
	for {
		select {
		case <-node.ctx.Done():
			return
		default:
			time.Sleep(time.Duration(sleepMillisecondTime) * time.Millisecond)
			node.SegmentStatistic(sleepMillisecondTime)
		}
	}
}
