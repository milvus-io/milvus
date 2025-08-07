package lazygrpc

import (
	"context"

	"google.golang.org/grpc"
)

// WithServiceCreator creates a lazy grpc service with a service creator.
func WithServiceCreator[T any](conn Conn, serviceCreator func(grpc.ClientConnInterface) T) Service[T] {
	return &serviceImpl[T]{
		Conn:           conn,
		serviceCreator: serviceCreator,
	}
}

// Service is a lazy grpc service.
type Service[T any] interface {
	Conn

	GetService(ctx context.Context) (T, error)
}

// serviceImpl is a lazy grpc service implementation.
type serviceImpl[T any] struct {
	Conn
	serviceCreator func(grpc.ClientConnInterface) T
}

func (s *serviceImpl[T]) GetService(ctx context.Context) (T, error) {
	conn, err := s.Conn.GetConn(ctx)
	if err != nil {
		var result T
		return result, err
	}
	return s.serviceCreator(conn), nil
}
