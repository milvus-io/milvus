package proxy

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
)

type mockMsgStream struct {
	msgstream.MsgStream
	asProducer func([]string)
	setRepack  func(repackFunc msgstream.RepackFunc)
	close      func()
}

func (m *mockMsgStream) AsProducer(producers []string) {
	if m.asProducer != nil {
		m.asProducer(producers)
	}
}

func (m *mockMsgStream) SetRepackFunc(repackFunc msgstream.RepackFunc) {
	if m.setRepack != nil {
		m.setRepack(repackFunc)
	}
}

func (m *mockMsgStream) Close() {
	if m.close != nil {
		m.close()
	}
}

func newMockMsgStream() *mockMsgStream {
	return &mockMsgStream{}
}

type mockMsgStreamFactory struct {
	msgstream.Factory
	f         func(ctx context.Context) (msgstream.MsgStream, error)
	fQStream  func(ctx context.Context) (msgstream.MsgStream, error)
	fTtStream func(ctx context.Context) (msgstream.MsgStream, error)
}

func (m *mockMsgStreamFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	if m.f != nil {
		return m.f(ctx)
	}
	return nil, errors.New("mock")
}

func (m *mockMsgStreamFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	if m.fTtStream != nil {
		return m.fTtStream(ctx)
	}
	return nil, errors.New("mock")
}

func (m *mockMsgStreamFactory) NewQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	if m.fQStream != nil {
		return m.fQStream(ctx)
	}
	return nil, errors.New("mock")
}

func newMockMsgStreamFactory() *mockMsgStreamFactory {
	return &mockMsgStreamFactory{}
}
