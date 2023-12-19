package io

import (
	"path"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"golang.org/x/net/context"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/conc"
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
	pool := conc.NewDefaultPool[any]()

	s.cm = storage.NewLocalChunkManager(storage.RootPath(binlogIOTestDir))

	s.b = NewBinlogIO(s.cm, pool)
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

func (s *BinlogIOSuite) TestJoinFullPath() {
	tests := []struct {
		description string
		inPaths     []string
		outPath     string
	}{
		{"no input", nil, path.Join(binlogIOTestDir)},
		{"input one", []string{"a"}, path.Join(binlogIOTestDir, "a")},
		{"input two", []string{"a", "b"}, path.Join(binlogIOTestDir, "a/b")},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			out := s.b.JoinFullPath(test.inPaths...)
			s.Equal(test.outPath, out)
		})
	}
}
