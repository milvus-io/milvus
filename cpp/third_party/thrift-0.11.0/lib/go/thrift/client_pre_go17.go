// +build !go1.7

package thrift

import "golang.org/x/net/context"

type TClient interface {
	Call(ctx context.Context, method string, args, result TStruct) error
}

func (p *TStandardClient) Call(ctx context.Context, method string, args, result TStruct) error {
	return p.call(method, args, result)
}
