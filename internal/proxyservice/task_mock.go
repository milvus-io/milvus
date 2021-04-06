package proxyservice

import "context"

type mockTask struct {
	ctx  context.Context
	id   UniqueID
	name string
}

func (t *mockTask) Ctx() context.Context {
	return t.ctx
}

func (t *mockTask) ID() UniqueID {
	return t.id
}

func (t *mockTask) Name() string {
	return t.name
}

func (t *mockTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *mockTask) Execute(ctx context.Context) error {
	return nil
}

func (t *mockTask) PostExecute(ctx context.Context) error {
	return nil
}

func (t *mockTask) WaitToFinish() error {
	return nil
}

func (t *mockTask) Notify(err error) {
}

func newMockTask(ctx context.Context) *mockTask {
	return &mockTask{
		ctx:  ctx,
		id:   0,
		name: "mockTask",
	}
}

type mockRegisterLinkTask struct {
	mockTask
}

type mockRegisterNodeTask struct {
	mockTask
}

type mockInvalidateCollectionMetaCacheTask struct {
	mockTask
}

func newMockRegisterLinkTask(ctx context.Context) *mockRegisterLinkTask {
	return &mockRegisterLinkTask{
		mockTask: mockTask{
			ctx:  ctx,
			id:   0,
			name: "mockRegisterLinkTask",
		},
	}
}

func newMockRegisterNodeTask(ctx context.Context) *mockRegisterNodeTask {
	return &mockRegisterNodeTask{
		mockTask: mockTask{
			ctx:  ctx,
			id:   0,
			name: "mockRegisterNodeTask",
		},
	}
}

func newMockInvalidateCollectionMetaCacheTask(ctx context.Context) *mockInvalidateCollectionMetaCacheTask {
	return &mockInvalidateCollectionMetaCacheTask{
		mockTask: mockTask{
			ctx:  ctx,
			id:   0,
			name: "mockInvalidateCollectionMetaCacheTask",
		},
	}
}
