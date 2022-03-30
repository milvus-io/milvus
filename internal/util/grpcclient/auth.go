package grpcclient

import (
	"context"
)

type Token struct {
	Value string
}

const headerAuthorize string = "authorization"

func (t *Token) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{headerAuthorize: t.Value}, nil
}

func (t *Token) RequireTransportSecurity() bool {
	return false
}
