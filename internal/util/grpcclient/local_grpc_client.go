package grpcclient

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ grpc.ClientConnInterface = &localConn{}

// NewLocalGRPCClient creates a grpc client that calls the server directly.
// !!! Warning: it didn't make any network or serialization/deserialization, so it's not promise concurrent safe.
// and there's no interceptor for client and server like the common grpc client/server.
func NewLocalGRPCClient[C any, S any](desc *grpc.ServiceDesc, server S, clientCreator func(grpc.ClientConnInterface) C) C {
	return clientCreator(&localConn{
		serviceDesc: desc,
		server:      server,
	})
}

// localConn is a grpc.ClientConnInterface implementation that calls the server directly.
type localConn struct {
	serviceDesc *grpc.ServiceDesc // ServiceDesc is the descriptor for this service.
	server      interface{}       // the server object.
}

// Invoke calls the server method directly.
func (c *localConn) Invoke(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
	methodDesc := c.findMethod(method)
	if methodDesc == nil {
		return status.Errorf(codes.Unimplemented, fmt.Sprintf("method %s not implemented", method))
	}
	resp, err := methodDesc.Handler(c.server, ctx, func(in any) error {
		reflect.ValueOf(in).Elem().Set(reflect.ValueOf(args).Elem())
		return nil
	}, nil)
	if err != nil {
		return err
	}
	reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(resp).Elem())
	return nil
}

// NewStream is not supported by now, wait for implementation.
func (c *localConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	panic("we don't support local stream rpc by now")
}

// findMethod finds the method descriptor by the full method name.
func (c *localConn) findMethod(fullMethodName string) *grpc.MethodDesc {
	strs := strings.SplitN(fullMethodName[1:], "/", 2)
	serviceName := strs[0]
	methodName := strs[1]
	if c.serviceDesc.ServiceName != serviceName {
		return nil
	}
	for i := range c.serviceDesc.Methods {
		if c.serviceDesc.Methods[i].MethodName == methodName {
			return &c.serviceDesc.Methods[i]
		}
	}
	return nil
}
