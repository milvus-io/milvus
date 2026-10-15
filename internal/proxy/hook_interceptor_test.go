package proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

type errorHook struct {
	hookutil.DefaultHook
	beforeErr error
	afterErr  error
}

func (h errorHook) Before(ctx context.Context, req interface{}, fullMethod string) (context.Context, error) {
	return ctx, h.beforeErr
}

func (h errorHook) After(ctx context.Context, resp interface{}, err error, fullMethod string) error {
	return h.afterErr
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

	hookutil.InitOnceHook()
	hookutil.SetTestHook(mockHoo)
	res, err = interceptor(ctx, "request", info, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	assert.Equal(t, res, mockHoo.mockRes)
	assert.Contains(t, err.Error(), mockHoo.mockErr.Error())
	res, err = interceptor(ctx, "request", emptyFullMethod, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	assert.Equal(t, res, mockHoo.mockRes)
	assert.Contains(t, err.Error(), mockHoo.mockErr.Error())

	hookutil.SetTestHook(beforeHoo)
	_, err = interceptor(ctx, r, info, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	assert.Equal(t, r.method, beforeHoo.method)
	assert.Contains(t, err.Error(), beforeHoo.err.Error())

	beforeHoo.err = nil
	hookutil.SetTestHook(beforeHoo)
	_, err = interceptor(ctx, r, info, func(ctx context.Context, req interface{}) (interface{}, error) {
		assert.Equal(t, beforeHoo.ctxValue, ctx.Value(beforeHoo.ctxKey))
		return nil, nil
	})
	assert.Equal(t, r.method, beforeHoo.method)
	assert.Nil(t, err)

	hookutil.SetTestHook(afterHoo)
	_, err = interceptor(ctx, r, info, func(ctx context.Context, r interface{}) (interface{}, error) {
		return re, nil
	})
	assert.Equal(t, re.method, afterHoo.method)
	assert.Contains(t, err.Error(), afterHoo.err.Error())

	hookutil.SetTestHook(&hookutil.DefaultHook{})
	res, err = interceptor(ctx, r, info, func(ctx context.Context, r interface{}) (interface{}, error) {
		return &resp{
			method: r.(*req).method,
		}, nil
	})
	assert.Equal(t, res.(*resp).method, r.method)
	assert.NoError(t, err)
}

func TestHookInterceptorDoesNotLogCredentialRequests(t *testing.T) {
	logs := captureProxyLogs(t)
	hookutil.InitOnceHook()
	t.Cleanup(func() {
		hookutil.SetTestHook(hookutil.DefaultHook{})
	})

	createPassword := "CREATE_PASSWORD_SENTINEL_DO_NOT_LOG"
	encodedCreatePassword := crypto.Base64Encode(createPassword)
	hookutil.SetTestHook(errorHook{beforeErr: errors.New("before hook failed")})
	_, err := HookInterceptor(
		context.Background(),
		&milvuspb.CreateCredentialRequest{Username: "alice", Password: encodedCreatePassword},
		"admin",
		"/milvus.proto.milvus.MilvusService/CreateCredential",
		func(ctx context.Context, req interface{}) (interface{}, error) { return nil, nil },
	)
	require.Error(t, err)

	oldPassword := "OLD_PASSWORD_SENTINEL_DO_NOT_LOG"
	newPassword := "NEW_PASSWORD_SENTINEL_DO_NOT_LOG"
	encodedOldPassword := crypto.Base64Encode(oldPassword)
	encodedNewPassword := crypto.Base64Encode(newPassword)
	hookutil.SetTestHook(errorHook{afterErr: errors.New("after hook failed")})
	_, err = HookInterceptor(
		context.Background(),
		&milvuspb.UpdateCredentialRequest{
			Username:    "alice",
			OldPassword: encodedOldPassword,
			NewPassword: encodedNewPassword,
		},
		"admin",
		"/milvus.proto.milvus.MilvusService/UpdateCredential",
		func(ctx context.Context, req interface{}) (interface{}, error) { return nil, nil },
	)
	require.Error(t, err)

	output := logs.String()
	for _, secret := range []string{
		createPassword,
		encodedCreatePassword,
		oldPassword,
		encodedOldPassword,
		newPassword,
		encodedNewPassword,
	} {
		assert.NotContains(t, output, secret)
	}
	lowerOutput := strings.ToLower(output)
	assert.NotContains(t, lowerOutput, "password:")
	assert.NotContains(t, lowerOutput, "oldpassword")
	assert.NotContains(t, lowerOutput, "newpassword")
}

func TestUpdateProxyFunctionCallMetric(t *testing.T) {
	assert.NotPanics(t, func() {
		updateProxyFunctionCallMetric("/milvus.proto.milvus.MilvusService/Flush", errors.New("mock hook error"))
		updateProxyFunctionCallMetric("Flush", merr.WrapErrParameterInvalidMsg("mock input error"))
		updateProxyFunctionCallMetric("", nil)
	})
}

// A refusal returned from Before reaches the client as InvalidArgument, not
// as codes.Unknown: a bare error would be retried by the SDK forever, which is
// what the note in hookError is about. A hook that needs the caller to see a
// classification answers from Mock instead.
func TestABeforeRefusalIsNotRetriableAtTheTransport(t *testing.T) {
	// Consume the lazy plugin init first, or the GetHook inside the
	// interceptor would run it and put the default hook back over this one.
	hookutil.InitOnceHook()
	hookutil.SetTestHook(beforeMock{err: merr.WrapErrServiceUnavailable("not ready")})
	defer hookutil.SetTestHook(hookutil.DefaultHook{})

	_, err := UnaryServerHookInterceptor()(context.Background(), &req{},
		&grpc.UnaryServerInfo{FullMethod: "insert"},
		func(ctx context.Context, req interface{}) (interface{}, error) { return nil, nil })
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.False(t, merr.IsMilvusError(err))
}
