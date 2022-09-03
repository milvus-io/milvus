package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap"
)

type GetCollectionNameFunc func(collID, partitionID UniqueID) (string, string, error)
type IDAllocator func(count uint32) (UniqueID, UniqueID, error)
type ImportFunc func(ctx context.Context, req *datapb.ImportTaskRequest) *datapb.ImportTaskResponse

type ImportFactory interface {
	NewGetCollectionNameFunc() GetCollectionNameFunc
	NewIDAllocator() IDAllocator
	NewImportFunc() ImportFunc
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

func NewImportFactory(c *Core) ImportFactory {
	return &ImportFactoryImpl{c: c}
}

func GetCollectionNameWithCore(c *Core) GetCollectionNameFunc {
	return func(collID, partitionID UniqueID) (string, string, error) {
		colName, err := c.meta.GetCollectionNameByID(collID)
		if err != nil {
			log.Error("Core failed to get collection name by id", zap.Int64("ID", collID), zap.Error(err))
			return "", "", err
		}

		partName, err := c.meta.GetPartitionNameByID(collID, partitionID, 0)
		if err != nil {
			log.Error("Core failed to get partition name by id", zap.Int64("ID", partitionID), zap.Error(err))
			return colName, "", err
		}

		return colName, partName, nil
	}
}

func IDAllocatorWithCore(c *Core) IDAllocator {
	return func(count uint32) (UniqueID, UniqueID, error) {
		return c.idAllocator.Alloc(count)
	}
}

func ImportFuncWithCore(c *Core) ImportFunc {
	return func(ctx context.Context, req *datapb.ImportTaskRequest) *datapb.ImportTaskResponse {
		// TODO: better to handle error here.
		resp, _ := c.broker.Import(ctx, req)
		return resp
	}
}
