package components

import (
	"context"
)

func NewMsgStreamService(ctx context.Context) (*MsgStream, error) {
	return nil, nil
}

type MsgStream struct {
}

func (ps *MsgStream) Run() error {
	return nil
}

func (ps *MsgStream) Stop() error {
	return nil
}
