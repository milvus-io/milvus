//go:build test
// +build test

package walimplstest

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
)

var _ walimpls.OpenerImpls = &opener{}

type opener struct{}

func (*opener) Open(ctx context.Context, opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	l := getOrCreateLogs(opt.Channel.Name)
	return &walImpls{
		WALHelper: *helper.NewWALHelper(opt),
		datas:     l,
	}, nil
}

func (*opener) Close() {
}
