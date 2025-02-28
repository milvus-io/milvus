package grpcclient

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/util"
)

type Token struct {
	Value string
}

func (t *Token) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{util.HeaderSourceID: t.Value}, nil
}

func (t *Token) RequireTransportSecurity() bool {
	return false
}
