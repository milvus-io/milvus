//go:build test
// +build test

package walimplstest

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/helper"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
)

var _ walimpls.OpenerImpls = &opener{}

type opener struct{}

func (*opener) Open(ctx context.Context, opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
	l := getOrCreateLogs(opt.Channel.GetName())
	return &walImpls{
		WALHelper: *helper.NewWALHelper(opt),
		datas:     l,
	}, nil
}

func (*opener) Close() {
}
