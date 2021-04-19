package dataservice

import (
	"context"
	"fmt"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"

	"github.com/zilliztech/milvus-distributed/internal/errors"
)

// TODO: get timestamp from timestampOracle

type task interface {
	Type() commonpb.MsgType
	Ts() (Timestamp, error)
	Execute() error
	WaitToFinish(ctx context.Context) error
	Notify(err error)
}
type baseTask struct {
	sch  *ddRequestScheduler
	meta *meta
	cv   chan error
}

func (bt *baseTask) Notify(err error) {
	bt.cv <- err
}

func (bt *baseTask) WaitToFinish(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Errorf("context done")
		case err, ok := <-bt.cv:
			if !ok {
				return errors.Errorf("notify chan closed")
			}
			return err
		}
	}
}

type allocateTask struct {
	baseTask
	req           *datapb.AssignSegIDRequest
	resp          *datapb.AssignSegIDResponse
	segAllocator  segmentAllocator
	insertCMapper insertChannelMapper
}

func (task *allocateTask) Type() commonpb.MsgType {
	return commonpb.MsgType_kAllocateSegment
}

func (task *allocateTask) Ts() (Timestamp, error) {
	return task.req.Timestamp, nil
}

func (task *allocateTask) Execute() error {
	for _, req := range task.req.SegIDRequests {
		result := &datapb.SegIDAssignment{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			},
		}
		segmentID, retCount, expireTs, err := task.segAllocator.AllocSegment(req.CollectionID, req.PartitionID, req.ChannelName, int(req.Count))
		if err != nil {
			if _, ok := err.(errRemainInSufficient); !ok {
				result.Status.Reason = fmt.Sprintf("allocation of Collection %d, Partition %d, Channel %s, Count %d error:  %s",
					req.CollectionID, req.PartitionID, req.ChannelName, req.Count, err.Error())
				task.resp.SegIDAssignments = append(task.resp.SegIDAssignments, result)
				continue
			}

			log.Printf("no enough space for allocation of Collection %d, Partition %d, Channel %s, Count %d",
				req.CollectionID, req.PartitionID, req.ChannelName, req.Count)
			if err = task.openNewSegment(req.CollectionID, req.PartitionID, req.ChannelName); err != nil {
				result.Status.Reason = fmt.Sprintf("open new segment of Collection %d, Partition %d, Channel %s, Count %d error:  %s",
					req.CollectionID, req.PartitionID, req.ChannelName, req.Count, err.Error())
				task.resp.SegIDAssignments = append(task.resp.SegIDAssignments, result)
				continue
			}
			segmentID, retCount, expireTs, err = task.segAllocator.AllocSegment(req.CollectionID, req.PartitionID, req.ChannelName, int(req.Count))
			if err != nil {
				result.Status.Reason = fmt.Sprintf("retry allocation of Collection %d, Partition %d, Channel %s, Count %d error:  %s",
					req.CollectionID, req.PartitionID, req.ChannelName, req.Count, err.Error())
				task.resp.SegIDAssignments = append(task.resp.SegIDAssignments, result)
				continue
			}
		}
		result.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
		result.CollectionID = req.CollectionID
		result.SegID = segmentID
		result.PartitionID = req.PartitionID
		result.Count = uint32(retCount)
		result.ExpireTime = expireTs
		result.ChannelName = req.ChannelName
		task.resp.SegIDAssignments = append(task.resp.SegIDAssignments, result)
	}
	return nil
}

func (task *allocateTask) openNewSegment(collectionID UniqueID, partitionID UniqueID, channelName string) error {
	cRange, err := task.insertCMapper.GetChannelRange(channelName)
	if err != nil {
		return err
	}

	segmentInfo, err := task.meta.BuildSegment(collectionID, partitionID, cRange)
	if err != nil {
		return err
	}

	if err = task.meta.AddSegment(segmentInfo); err != nil {
		return err
	}
	if err = task.segAllocator.OpenSegment(collectionID, partitionID, segmentInfo.SegmentID, segmentInfo.InsertChannels); err != nil {
		return err
	}
	return nil
}
