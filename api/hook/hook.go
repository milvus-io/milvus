package hook

import "context"

type Hook interface {
	Init(params map[string]string) error
	Mock(ctx context.Context, req interface{}, fullMethod string) (bool, interface{}, error)
	Before(ctx context.Context, req interface{}, fullMethod string) error
	After(ctx context.Context, result interface{}, err error, fullMethod string) error
	Release()
}
