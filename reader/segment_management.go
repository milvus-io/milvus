package reader

import (
	"fmt"
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

func (node *QueryNode) SegmentService() {
	for {
		time.Sleep(200 * time.Millisecond)
		node.SegmentsManagement()
		fmt.Println("do segments management in 200ms")
	}
}
