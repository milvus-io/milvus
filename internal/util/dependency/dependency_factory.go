package dependency

import (
	"context"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
)

type StandAloneDependencyFactory struct {
	msgstream.MsgFactory
	storage.ChunkFactory
}

func NewStandAloneDependencyFactory() *StandAloneDependencyFactory {
	return &StandAloneDependencyFactory{
		MsgFactory:   msgstream.NewRmsFactory(),
		ChunkFactory: storage.NewStandAloneChunkManagerFactory(),
	}
}

type DistributedDependencyFactory struct {
	msgstream.MsgFactory
	storage.ChunkFactory
}

func NewDistributedDependencyFactory() *DistributedDependencyFactory {
	return &DistributedDependencyFactory{
		MsgFactory:   msgstream.NewPmsFactory(),
		ChunkFactory: storage.NewDistributedChunkManagerFactory(),
	}
}

// Factory is an interface that can be used to generate a new dependency object
type Factory interface {
	SetParams(params map[string]interface{}) error
	NewMsgStream(ctx context.Context) (msgstream.MsgStream, error)
	NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error)
	NewQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error)
	NewLocalChunkManager(path string) (storage.ChunkManager, error)
	NewRemoteChunkManager(ctx context.Context, opts ...storage.Option) (storage.ChunkManager, error)
}
