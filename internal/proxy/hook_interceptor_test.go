package proxy

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"

	"github.com/stretchr/testify/assert"
)

func TestInitHook(t *testing.T) {
	Params.ProxyCfg.SoPath = ""
	initHook()
	assert.IsType(t, defaultHook{}, hoo)

	Params.ProxyCfg.SoPath = "/a/b/hook.so"
	assert.Panics(t, func() {
		initHook()
	})
	Params.ProxyCfg.SoPath = ""
}

type mockHook struct {
	defaultHook
	mockRes interface{}
	mockErr error
}

func (m mockHook) Mock(req interface{}, fullMethod string) (bool, interface{}, error) {
	return true, m.mockRes, m.mockErr
}

type req struct {
	method string
}

type beforeMock struct {
	defaultHook
	method string
	err    error
}

func (b beforeMock) Before(r interface{}, fullMethod string) error {
	re, ok := r.(*req)
	if !ok {
		return errors.New("r is invalid type")
	}
	re.method = b.method
	return b.err
}

type resp struct {
	method string
}

type afterMock struct {
	defaultHook
	method string
	err    error
}

func (a afterMock) After(r interface{}, err error, fullMethod string) error {
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
		interceptor = UnaryServerHookInterceptor()
		mockHoo     = mockHook{mockRes: "mock", mockErr: errors.New("mock")}
		r           = &req{method: "req"}
		re          = &resp{method: "resp"}
		beforeHoo   = beforeMock{method: "before", err: errors.New("before")}
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

	hoo = beforeHoo
	_, err = interceptor(ctx, r, info, func(ctx context.Context, req interface{}) (interface{}, error) {
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
