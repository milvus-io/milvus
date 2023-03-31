package msgstream

import "context"

type MockMqFactory struct {
	Factory
	NewMsgStreamFunc func(ctx context.Context) (MsgStream, error)
}

func NewMockMqFactory() *MockMqFactory {
	return &MockMqFactory{}
}

func (m MockMqFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	return m.NewMsgStreamFunc(ctx)
}
