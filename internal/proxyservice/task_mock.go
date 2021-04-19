// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

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
