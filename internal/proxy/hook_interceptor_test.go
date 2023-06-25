package proxy

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
)

func TestInitHook(t *testing.T) {
	paramtable.Get().Save(Params.ProxyCfg.SoPath.Key, "")
	initHook()
	assert.IsType(t, defaultHook{}, hoo)

	paramtable.Get().Save(Params.ProxyCfg.SoPath.Key, "/a/b/hook.so")
	err := initHook()
	assert.Error(t, err)
	paramtable.Get().Save(Params.ProxyCfg.SoPath.Key, "")
}

type mockHook struct {
	defaultHook
	mockRes interface{}
	mockErr error
}

func (m mockHook) Mock(ctx context.Context, req interface{}, fullMethod string) (bool, interface{}, error) {
	return true, m.mockRes, m.mockErr
}

type req struct {
	method string
}

type BeforeMockCtxKey int

type beforeMock struct {
	defaultHook
	method   string
	ctxKey   BeforeMockCtxKey
	ctxValue string
	err      error
}

func (b beforeMock) Before(ctx context.Context, r interface{}, fullMethod string) (context.Context, error) {
	re, ok := r.(*req)
	if !ok {
		return ctx, errors.New("r is invalid type")
	}
	re.method = b.method
	return context.WithValue(ctx, b.ctxKey, b.ctxValue), b.err
}

type resp struct {
	method string
}

type afterMock struct {
	defaultHook
	method string
	err    error
}

func (a afterMock) After(ctx context.Context, r interface{}, err error, fullMethod string) error {
	re, ok := r.(*resp)
	if !ok {
		return errors.New("r is invalid type")
	}
	re.method = a.method
	return a.err
}

func TestHookInterceptor(t *testing.T) {
	var (
		ctx  = context.Background()
		info = &grpc.UnaryServerInfo{
			FullMethod: "test",
		}
		emptyFullMethod = &grpc.UnaryServerInfo{
			FullMethod: "",
		}
		interceptor = UnaryServerHookInterceptor()
		mockHoo     = mockHook{mockRes: "mock", mockErr: errors.New("mock")}
		r           = &req{method: "req"}
		re          = &resp{method: "resp"}
		beforeHoo   = beforeMock{method: "before", ctxKey: 100, ctxValue: "hook", err: errors.New("before")}
		afterHoo    = afterMock{method: "after", err: errors.New("after")}

		res interface{}
		err error
	)

	hoo = mockHoo
	res, err = interceptor(ctx, "request", info, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	assert.Equal(t, res, mockHoo.mockRes)
	assert.Equal(t, err, mockHoo.mockErr)
	res, err = interceptor(ctx, "request", emptyFullMethod, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	assert.Equal(t, res, mockHoo.mockRes)
	assert.Equal(t, err, mockHoo.mockErr)

	hoo = beforeHoo
	_, err = interceptor(ctx, r, info, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	assert.Equal(t, r.method, beforeHoo.method)
	assert.Equal(t, err, beforeHoo.err)

	beforeHoo.err = nil
	hoo = beforeHoo
	_, err = interceptor(ctx, r, info, func(ctx context.Context, req interface{}) (interface{}, error) {
		assert.Equal(t, beforeHoo.ctxValue, ctx.Value(beforeHoo.ctxKey))
		return nil, nil
	})
	assert.Equal(t, r.method, beforeHoo.method)
	assert.Equal(t, err, beforeHoo.err)

	hoo = afterHoo
	_, err = interceptor(ctx, r, info, func(ctx context.Context, r interface{}) (interface{}, error) {
		return re, nil
	})
	assert.Equal(t, re.method, afterHoo.method)
	assert.Equal(t, err, afterHoo.err)

	hoo = defaultHook{}
	res, err = interceptor(ctx, r, info, func(ctx context.Context, r interface{}) (interface{}, error) {
		return &resp{
			method: r.(*req).method,
		}, nil
	})
	assert.Equal(t, res.(*resp).method, r.method)
	assert.NoError(t, err)
}

func TestDefaultHook(t *testing.T) {
	d := defaultHook{}
	assert.NoError(t, d.Init(nil))
	assert.NotPanics(t, func() {
		d.Release()
	})
}
