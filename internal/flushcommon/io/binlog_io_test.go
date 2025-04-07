package io

import (
	"path"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"golang.org/x/net/context"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const binlogIOTestDir = "/tmp/milvus_test/binlog_io"

func TestBinlogIO(t *testing.T) {
	suite.Run(t, new(BinlogIOSuite))
}

type BinlogIOSuite struct {
	suite.Suite

	cm storage.ChunkManager
	b  BinlogIO
}

func (s *BinlogIOSuite) SetupTest() {
	paramtable.Init()
	s.cm = storage.NewLocalChunkManager(objectstorage.RootPath(binlogIOTestDir))

	s.b = NewBinlogIO(s.cm)
}

func (s *BinlogIOSuite) TeardownTest() {
	ctx := context.Background()
	s.cm.RemoveWithPrefix(ctx, s.cm.RootPath())
}

func (s *BinlogIOSuite) TestUploadDownload() {
	kvs := map[string][]byte{
		path.Join(binlogIOTestDir, "a/b/c"): {1, 255, 255},
		path.Join(binlogIOTestDir, "a/b/d"): {1, 255, 255},
	}

	ctx := context.Background()
	err := s.b.Upload(ctx, kvs)
	s.NoError(err)

	vs, err := s.b.Download(ctx, lo.Keys(kvs))
	s.NoError(err)
	s.ElementsMatch(lo.Values(kvs), vs)
}
