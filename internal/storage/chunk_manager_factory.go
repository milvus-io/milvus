package storage

import (
	"context"
)

type StandAloneChunkManagerFactory struct {
}

func NewStandAloneChunkManagerFactory() *StandAloneChunkManagerFactory {
	return &StandAloneChunkManagerFactory{}
}

func (f *StandAloneChunkManagerFactory) NewLocalChunkManager(path string) (ChunkManager, error) {
	return NewLocalChunkManager(RootPath(path)), nil
}

func (f *StandAloneChunkManagerFactory) NewRemoteChunkManager(ctx context.Context, opts ...Option) (ChunkManager, error) {
	return NewLocalChunkManager(opts...), nil
}

type DistributedChunkManagerFactory struct {
}

func NewDistributedChunkManagerFactory() *DistributedChunkManagerFactory {
	return &DistributedChunkManagerFactory{}
}

func (f *DistributedChunkManagerFactory) NewLocalChunkManager(path string) (ChunkManager, error) {
	return NewLocalChunkManager(RootPath(path)), nil
}

func (f *DistributedChunkManagerFactory) NewRemoteChunkManager(ctx context.Context, opts ...Option) (ChunkManager, error) {
	return NewMinioChunkManager(ctx, opts...)
}

type ChunkFactory interface {
	NewLocalChunkManager(path string) (ChunkManager, error)
	NewRemoteChunkManager(ctx context.Context, opts ...Option) (ChunkManager, error)
}
