package proxy

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/hookutil"
)

type mockHook struct {
	hookutil.DefaultHook
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
	hookutil.DefaultHook
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
	hookutil.DefaultHook
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

	hookutil.SetTestHook(mockHoo)
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

	hookutil.SetTestHook(beforeHoo)
	_, err = interceptor(ctx, r, info, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	assert.Equal(t, r.method, beforeHoo.method)
	assert.Equal(t, err, beforeHoo.err)

	beforeHoo.err = nil
	hookutil.SetTestHook(beforeHoo)
	_, err = interceptor(ctx, r, info, func(ctx context.Context, req interface{}) (interface{}, error) {
		assert.Equal(t, beforeHoo.ctxValue, ctx.Value(beforeHoo.ctxKey))
		return nil, nil
	})
	assert.Equal(t, r.method, beforeHoo.method)
	assert.Equal(t, err, beforeHoo.err)

	hookutil.SetTestHook(afterHoo)
	_, err = interceptor(ctx, r, info, func(ctx context.Context, r interface{}) (interface{}, error) {
		return re, nil
	})
	assert.Equal(t, re.method, afterHoo.method)
	assert.Equal(t, err, afterHoo.err)

	hookutil.SetTestHook(&hookutil.DefaultHook{})
	res, err = interceptor(ctx, r, info, func(ctx context.Context, r interface{}) (interface{}, error) {
		return &resp{
			method: r.(*req).method,
		}, nil
	})
	assert.Equal(t, res.(*resp).method, r.method)
	assert.NoError(t, err)
}

func TestUpdateProxyFunctionCallMetric(t *testing.T) {
	assert.NotPanics(t, func() {
		updateProxyFunctionCallMetric("/milvus.proto.milvus.MilvusService/Flush")
		updateProxyFunctionCallMetric("Flush")
		updateProxyFunctionCallMetric("")
	})
}
