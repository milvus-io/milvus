package rootcoord

import (
	"context"
)

type taskV2 interface {
	GetCtx() context.Context
	SetCtx(context.Context)
	SetTs(ts Timestamp)
	GetTs() Timestamp
	SetID(id UniqueID)
	GetID() UniqueID
	Prepare(ctx context.Context) error
	Execute(ctx context.Context) error
	WaitToFinish() error
	NotifyDone(err error)
}

type baseTaskV2 struct {
	ctx  context.Context
	core *Core
	done chan error
	ts   Timestamp
	id   UniqueID
}

func (b *baseTaskV2) SetCtx(ctx context.Context) {
	b.ctx = ctx
}

func (b *baseTaskV2) GetCtx() context.Context {
	return b.ctx
}

func (b *baseTaskV2) SetTs(ts Timestamp) {
	b.ts = ts
}

func (b *baseTaskV2) GetTs() Timestamp {
	return b.ts
}

func (b *baseTaskV2) SetID(id UniqueID) {
	b.id = id
}

func (b *baseTaskV2) GetID() UniqueID {
	return b.id
}

func (b *baseTaskV2) Prepare(ctx context.Context) error {
	return nil
}

func (b *baseTaskV2) Execute(ctx context.Context) error {
	return nil
}

func (b *baseTaskV2) WaitToFinish() error {
	return <-b.done
}

func (b *baseTaskV2) NotifyDone(err error) {
	b.done <- err
}
