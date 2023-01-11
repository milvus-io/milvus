package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/indexpb"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type GetCollectionNameFunc func(collID, partitionID UniqueID) (string, string, error)
type IDAllocator func(count uint32) (UniqueID, UniqueID, error)
type ImportFunc func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error)
type MarkSegmentsDroppedFunc func(ctx context.Context, segIDs []int64) (*commonpb.Status, error)
type GetSegmentStatesFunc func(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error)
type DescribeIndexFunc func(ctx context.Context, colID UniqueID) (*indexpb.DescribeIndexResponse, error)
type GetSegmentIndexStateFunc func(ctx context.Context, collID UniqueID, indexName string, segIDs []UniqueID) ([]*indexpb.SegmentIndexState, error)
type UnsetIsImportingStateFunc func(context.Context, *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error)

type ImportFactory interface {
	NewGetCollectionNameFunc() GetCollectionNameFunc
	NewIDAllocator() IDAllocator
	NewImportFunc() ImportFunc
	NewMarkSegmentsDroppedFunc() MarkSegmentsDroppedFunc
	NewGetSegmentStatesFunc() GetSegmentStatesFunc
	NewDescribeIndexFunc() DescribeIndexFunc
	NewGetSegmentIndexStateFunc() GetSegmentIndexStateFunc
	NewUnsetIsImportingStateFunc() UnsetIsImportingStateFunc
}

type ImportFactoryImpl struct {
	c *Core
}

func (f ImportFactoryImpl) NewGetCollectionNameFunc() GetCollectionNameFunc {
	return GetCollectionNameWithCore(f.c)
}

func (f ImportFactoryImpl) NewIDAllocator() IDAllocator {
	return IDAllocatorWithCore(f.c)
}

func (f ImportFactoryImpl) NewImportFunc() ImportFunc {
	return ImportFuncWithCore(f.c)
}

func (f ImportFactoryImpl) NewMarkSegmentsDroppedFunc() MarkSegmentsDroppedFunc {
	return MarkSegmentsDroppedWithCore(f.c)
}

func (f ImportFactoryImpl) NewGetSegmentStatesFunc() GetSegmentStatesFunc {
	return GetSegmentStatesWithCore(f.c)
}

func (f ImportFactoryImpl) NewDescribeIndexFunc() DescribeIndexFunc {
	return DescribeIndexWithCore(f.c)
}

func (f ImportFactoryImpl) NewGetSegmentIndexStateFunc() GetSegmentIndexStateFunc {
	return GetSegmentIndexStateWithCore(f.c)
}

func (f ImportFactoryImpl) NewUnsetIsImportingStateFunc() UnsetIsImportingStateFunc {
	return UnsetIsImportingStateWithCore(f.c)
}

func NewImportFactory(c *Core) ImportFactory {
	return &ImportFactoryImpl{c: c}
}

func GetCollectionNameWithCore(c *Core) GetCollectionNameFunc {
	return func(collID, partitionID UniqueID) (string, string, error) {
		colInfo, err := c.meta.GetCollectionByID(c.ctx, collID, typeutil.MaxTimestamp, false)
		if err != nil {
			log.Error("Core failed to get collection name by id", zap.Int64("ID", collID), zap.Error(err))
			return "", "", err
		}
		partName, err := c.meta.GetPartitionNameByID(collID, partitionID, 0)
		if err != nil {
			log.Error("Core failed to get partition name by id", zap.Int64("ID", partitionID), zap.Error(err))
			return colInfo.Name, "", err
		}

		return colInfo.Name, partName, nil
	}
}

func IDAllocatorWithCore(c *Core) IDAllocator {
	return func(count uint32) (UniqueID, UniqueID, error) {
		return c.idAllocator.Alloc(count)
	}
}

func ImportFuncWithCore(c *Core) ImportFunc {
	return func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		return c.broker.Import(ctx, req)
	}
}

func MarkSegmentsDroppedWithCore(c *Core) MarkSegmentsDroppedFunc {
	return func(ctx context.Context, segIDs []int64) (*commonpb.Status, error) {
		return c.broker.MarkSegmentsDropped(ctx, &datapb.MarkSegmentsDroppedRequest{
			SegmentIds: segIDs,
		})
	}
}

func GetSegmentStatesWithCore(c *Core) GetSegmentStatesFunc {
	return func(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
		return c.broker.GetSegmentStates(ctx, req)
	}
}

func DescribeIndexWithCore(c *Core) DescribeIndexFunc {
	return func(ctx context.Context, colID UniqueID) (*indexpb.DescribeIndexResponse, error) {
		return c.broker.DescribeIndex(ctx, colID)
	}
}

func GetSegmentIndexStateWithCore(c *Core) GetSegmentIndexStateFunc {
	return func(ctx context.Context, collID UniqueID, indexName string, segIDs []UniqueID) ([]*indexpb.SegmentIndexState, error) {
		return c.broker.GetSegmentIndexState(ctx, collID, indexName, segIDs)
	}
}

func UnsetIsImportingStateWithCore(c *Core) UnsetIsImportingStateFunc {
	return func(ctx context.Context, req *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error) {
		return c.broker.UnsetIsImportingState(ctx, req)
	}
}
