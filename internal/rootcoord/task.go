package rootcoord

import (
	"context"
)

type task interface {
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

type baseTask struct {
	ctx  context.Context
	core *Core
	done chan error
	ts   Timestamp
	id   UniqueID
}

func newBaseTask(ctx context.Context, core *Core) baseTask {
	b := baseTask{
		core: core,
		done: make(chan error, 1),
	}
	b.SetCtx(ctx)
	return b
}

func (b *baseTask) SetCtx(ctx context.Context) {
	b.ctx = ctx
}

func (b *baseTask) GetCtx() context.Context {
	return b.ctx
}

func (b *baseTask) SetTs(ts Timestamp) {
	b.ts = ts
}

func (b *baseTask) GetTs() Timestamp {
	return b.ts
}

func (b *baseTask) SetID(id UniqueID) {
	b.id = id
}

func (b *baseTask) GetID() UniqueID {
	return b.id
}

func (b *baseTask) Prepare(ctx context.Context) error {
	return nil
}

func (b *baseTask) Execute(ctx context.Context) error {
	return nil
}

func (b *baseTask) WaitToFinish() error {
	return <-b.done
}

func (b *baseTask) NotifyDone(err error) {
	b.done <- err
}
