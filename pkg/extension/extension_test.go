// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package extension

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
)

// stubHook is a hook.Hook that answers nothing; the tests only ask whether it
// is the one installed.
type stubHook struct{ name string }

func (stubHook) Init(map[string]string) error { return nil }
func (stubHook) Mock(context.Context, interface{}, string) (bool, interface{}, error) {
	return false, nil, nil
}

func (stubHook) Before(ctx context.Context, _ interface{}, _ string) (context.Context, error) {
	return ctx, nil
}
func (stubHook) After(context.Context, interface{}, error, string) error { return nil }
func (stubHook) VerifyAPIKey(string) (string, error)                     { return "", nil }
func (stubHook) Release()                                                {}

var _ hook.Hook = stubHook{}

func TestNothingIsInstalledByDefault(t *testing.T) {
	ResetForTest()
	assert.Nil(t, InstalledHook(), "a stock binary has no compiled-in hook")
	assert.Nil(t, InstalledCoordinatorEngine(), "a stock binary has no coordinator engine")
}

func TestSetHookInstallsTheHook(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)
	SetHook(stubHook{name: "form"})
	assert.Equal(t, stubHook{name: "form"}, InstalledHook())
	assert.Nil(t, InstalledCoordinatorEngine(), "installing a hook must not conjure an engine")
}

func TestSetCoordinatorEngineInstallsTheEngine(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)
	engine := &fakeCoordinatorEngine{}
	SetCoordinatorEngine(engine)
	assert.Same(t, engine, InstalledCoordinatorEngine())
	assert.Nil(t, InstalledHook(), "installing an engine must not conjure a hook")
}

func TestSetNilLeavesNothingInstalled(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)
	SetHook(stubHook{name: "form"})
	SetCoordinatorEngine(&fakeCoordinatorEngine{})
	SetHook(nil)
	SetCoordinatorEngine(nil)
	assert.Nil(t, InstalledHook())
	assert.Nil(t, InstalledCoordinatorEngine())
}
