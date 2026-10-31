package lance

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type innerReader interface {
	Size() (int64, error)
	Read() (*storage.InsertData, error)
	Close()
}

type reader struct {
	ctx        context.Context
	cm         storage.ChunkManager
	schema     *schemapb.CollectionSchema
	bufferSize int
	version    uint64

	inner    innerReader
	tempPath string
}

func NewReader(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema,
	path string, version uint64, bufferSize int) (*reader, error) {
	return nil, merr.WrapErrImportFailedMsg("lance import not implemented")
}

func (r *reader) Size() (int64, error)               { return 0, nil }
func (r *reader) Read() (*storage.InsertData, error) { return nil, nil }
func (r *reader) Close()                             {}
