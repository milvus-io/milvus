package reader

import (
	"fmt"
	masterPb "github.com/czs007/suvlim/pkg/master/grpc/master"
	msgPb "github.com/czs007/suvlim/pkg/master/grpc/message"
	"log"
	"strconv"
	"time"
)

func (node *QueryNode) SegmentsManagement() {
	node.queryNodeTimeSync.UpdateTSOTimeSync()
	var timeNow = node.queryNodeTimeSync.TSOTimeSync
	for _, collection := range node.Collections {
		for _, partition := range collection.Partitions {
			for _, oldSegment := range partition.OpenedSegments {
				// TODO: check segment status
				if timeNow >= oldSegment.SegmentCloseTime {
					// start new segment and add it into partition.OpenedSegments
					// TODO: get segmentID from master
					var segmentID int64 = 0
					var newSegment = partition.NewSegment(segmentID)
					newSegment.SegmentCloseTime = timeNow + SegmentLifetime
					partition.OpenedSegments = append(partition.OpenedSegments, newSegment)
					node.SegmentsMap[segmentID] = newSegment

					// close old segment and move it into partition.ClosedSegments
					// TODO: check status
					var _ = oldSegment.Close()
					partition.ClosedSegments = append(partition.ClosedSegments, oldSegment)
				}
			}
		}
	}
}

func (node *QueryNode) SegmentManagementService() {
	for {
		sleepMillisecondTime := 200
		time.Sleep(time.Duration(sleepMillisecondTime) * time.Millisecond)
		node.SegmentsManagement()
		fmt.Println("do segments management in ", strconv.Itoa(sleepMillisecondTime), "ms")
	}
}

func (node *QueryNode) SegmentStatistic(sleepMillisecondTime int) {
	var statisticData = make([]masterPb.SegmentStat, 0)

	for segmentID, segment := range node.SegmentsMap {
		currentMemSize := segment.GetMemSize()
		memIncreaseRate := float32(currentMemSize-segment.LastMemSize) / (float32(sleepMillisecondTime) / 1000)
		stat := masterPb.SegmentStat{
			// TODO: set master pb's segment id type from uint64 to int64
			SegmentId:  uint64(segmentID),
			MemorySize: currentMemSize,
			MemoryRate: memIncreaseRate,
		}
		statisticData = append(statisticData, stat)
	}

	var status = node.PublicStatistic(&statisticData)
	if status.ErrorCode != msgPb.ErrorCode_SUCCESS {
		log.Printf("Publish segments statistic failed")
	}
}

func (node *QueryNode) SegmentStatisticService() {
	for {
		sleepMillisecondTime := 1000
		time.Sleep(time.Duration(sleepMillisecondTime) * time.Millisecond)
		node.SegmentStatistic(sleepMillisecondTime)
		fmt.Println("do segments statistic in ", strconv.Itoa(sleepMillisecondTime), "ms")
	}
}
