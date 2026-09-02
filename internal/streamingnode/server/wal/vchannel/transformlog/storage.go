package transformlog

import (
	"context"
	"path"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type Store interface {
	WriteTransformLogChunk(ctx context.Context, vchannel string, chunk *streamingpb.TransformLogChunk) error
	ReadTransformLogChunk(ctx context.Context, vchannel string, chunkID uint64) (*streamingpb.TransformLogChunk, error)
}

type objectChunkStore struct {
	chunkManager storage.ChunkManager
	pchannel     string
}

func NewObjectChunkStore(chunkManager storage.ChunkManager, pchannel string) Store {
	return &objectChunkStore{
		chunkManager: chunkManager,
		pchannel:     pchannel,
	}
}

func (s *objectChunkStore) WriteTransformLogChunk(ctx context.Context, vchannel string, chunk *streamingpb.TransformLogChunk) error {
	bytes, err := proto.Marshal(chunk)
	if err != nil {
		return err
	}
	return s.chunkManager.Write(ctx, s.chunkPath(vchannel, chunk.GetChunkId()), bytes)
}

func (s *objectChunkStore) ReadTransformLogChunk(ctx context.Context, vchannel string, chunkID uint64) (*streamingpb.TransformLogChunk, error) {
	bytes, err := s.chunkManager.Read(ctx, s.chunkPath(vchannel, chunkID))
	if err != nil {
		return nil, err
	}
	chunk := &streamingpb.TransformLogChunk{}
	if err := proto.Unmarshal(bytes, chunk); err != nil {
		return nil, err
	}
	return chunk, nil
}

func (s *objectChunkStore) chunkPath(vchannel string, chunkID uint64) string {
	return path.Join(
		s.chunkManager.RootPath(),
		"transform-log",
		s.pchannel,
		vchannel,
		"chunks",
		strconv.FormatUint(chunkID, 10)+".pb",
	)
}
