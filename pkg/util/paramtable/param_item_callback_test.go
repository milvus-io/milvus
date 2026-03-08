// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paramtable

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/config"
)

func TestParamItem_RegisterCallback(t *testing.T) {
	manager := config.NewManager()
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}
	param.Init(manager)

	callback := func(ctx context.Context, key, oldValue, newValue string) error {
		return nil
	}

	param.RegisterCallback(callback)
	assert.NotNil(t, param.callback)

	param.UnregisterCallback()
	assert.Nil(t, param.callback)
}

func TestParamItem_CallbackTriggered(t *testing.T) {
	manager := config.NewManager()
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}
	param.Init(manager)

	var callbackCalled bool
	var capturedOldValue, capturedNewValue string
	var capturedKey string

	callback := func(ctx context.Context, key, oldValue, newValue string) error {
		callbackCalled = true
		capturedKey = key
		capturedOldValue = oldValue
		capturedNewValue = newValue
		return nil
	}

	param.RegisterCallback(callback)

	assert.Equal(t, "default", param.GetValue())

	event := &config.Event{
		EventType: config.UpdateType,
		Key:       "test.param",
		Value:     "new-value",
	}

	param.handleConfigChange(event)

	assert.True(t, callbackCalled)
	assert.Equal(t, "test.param", capturedKey)
	assert.Equal(t, "default", capturedOldValue)
	assert.Equal(t, "new-value", capturedNewValue)
}

func TestParamItem_CallbackErrorHandling(t *testing.T) {
	manager := config.NewManager()
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}
	param.Init(manager)

	var callbackCalled bool

	errorCallback := func(ctx context.Context, key, oldValue, newValue string) error {
		callbackCalled = true
		return fmt.Errorf("callback error")
	}

	param.RegisterCallback(errorCallback)

	event := &config.Event{
		EventType: config.UpdateType,
		Key:       "test.param",
		Value:     "new-value",
	}

	param.handleConfigChange(event)

	assert.True(t, callbackCalled)
}

func TestParamItem_NoValueChange(t *testing.T) {
	manager := config.NewManager()
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}
	param.Init(manager)

	var callbackCalled bool

	callback := func(ctx context.Context, key, oldValue, newValue string) error {
		callbackCalled = true
		return nil
	}

	param.RegisterCallback(callback)

	event := &config.Event{
		EventType: config.UpdateType,
		Key:       "test.param",
		Value:     "default",
	}

	param.handleConfigChange(event)

	assert.False(t, callbackCalled)
}

func TestParamItem_CallbackWithContext(t *testing.T) {
	manager := config.NewManager()
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}
	param.Init(manager)

	var callbackCalled bool

	callback := func(ctx context.Context, key, oldValue, newValue string) error {
		callbackCalled = true
		assert.NotNil(t, ctx)
		return nil
	}

	param.RegisterCallback(callback)

	event := &config.Event{
		EventType: config.UpdateType,
		Key:       "test.param",
		Value:     "new-value",
	}

	param.handleConfigChange(event)

	assert.True(t, callbackCalled)
}

func TestParamItem_CallbackCleanup(t *testing.T) {
	manager := config.NewManager()
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}
	param.Init(manager)

	var callbackCalled bool

	callback := func(ctx context.Context, key, oldValue, newValue string) error {
		callbackCalled = true
		return nil
	}

	param.RegisterCallback(callback)

	param.UnregisterCallback()

	event := &config.Event{
		EventType: config.UpdateType,
		Key:       "test.param",
		Value:     "new-value",
	}

	param.handleConfigChange(event)

	assert.False(t, callbackCalled)
}

func TestParamItem_ManagerNil(t *testing.T) {
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}

	callback := func(ctx context.Context, key, oldValue, newValue string) error {
		return nil
	}

	param.RegisterCallback(callback)
	assert.NotNil(t, param.callback)

	param.UnregisterCallback()
	assert.Nil(t, param.callback)
}

func TestParamItem_DispatcherNil(t *testing.T) {
	manager := config.NewManager()
	manager.Dispatcher = nil

	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}

	param.Init(manager)

	callback := func(ctx context.Context, key, oldValue, newValue string) error {
		return nil
	}

	param.RegisterCallback(callback)
	assert.NotNil(t, param.callback)
}

func TestParamItem_StressTest(t *testing.T) {
	manager := config.NewManager()
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}
	param.Init(manager)

	var callbackCount int

	callback := func(ctx context.Context, key, oldValue, newValue string) error {
		callbackCount++
		return nil
	}

	param.RegisterCallback(callback)

	for i := 0; i < 10; i++ {
		event := &config.Event{
			EventType: config.UpdateType,
			Key:       "test.param",
			Value:     fmt.Sprintf("value-%d", i),
		}
		param.handleConfigChange(event)
	}

	assert.Equal(t, 10, callbackCount)
}

func TestParamItem_FormatterWithCallback(t *testing.T) {
	manager := config.NewManager()
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "100",
		Formatter: func(v string) string {
			if v == "100" {
				return "formatted-100"
			}
			return v
		},
	}
	param.Init(manager)

	var callbackCalled bool
	var callbackNewValue string

	callback := func(ctx context.Context, key, oldValue, newValue string) error {
		callbackCalled = true
		callbackNewValue = newValue
		return nil
	}

	param.RegisterCallback(callback)

	event := &config.Event{
		EventType: config.UpdateType,
		Key:       "test.param",
		Value:     "200",
	}

	param.handleConfigChange(event)

	assert.True(t, callbackCalled)
	assert.Equal(t, "200", callbackNewValue)
}

func TestParamItem_InitWithManager(t *testing.T) {
	manager := config.NewManager()
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}

	assert.Nil(t, param.manager)

	param.Init(manager)

	assert.NotNil(t, param.manager)
	assert.Equal(t, manager, param.manager)

	assert.NotNil(t, manager.Dispatcher)
}

func TestParamItem_LastValueTracking_Change(t *testing.T) {
	manager := config.NewManager()
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}
	param.Init(manager)

	initialValue := param.GetValue()
	assert.Equal(t, "default", initialValue)

	lastVal := param.lastValue.Load()
	assert.NotNil(t, lastVal)
	assert.Equal(t, "default", *lastVal)

	var callbackNewValue string

	callback := func(ctx context.Context, key, oldValue, newValue string) error {
		callbackNewValue = newValue
		return nil
	}

	param.RegisterCallback(callback)

	event := &config.Event{
		EventType: config.UpdateType,
		Key:       "test.param",
		Value:     "new-value",
	}

	param.handleConfigChange(event)

	lastVal = param.lastValue.Load()
	assert.NotNil(t, lastVal)
	assert.Equal(t, "new-value", *lastVal)
	assert.Equal(t, "new-value", callbackNewValue)
}

func TestParamItem_LastValueTracking_NoChange(t *testing.T) {
	manager := config.NewManager()
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}
	param.Init(manager)

	initialValue := param.GetValue()
	assert.Equal(t, "default", initialValue)

	lastVal := param.lastValue.Load()
	assert.NotNil(t, lastVal)
	assert.Equal(t, "default", *lastVal)

	event := &config.Event{
		EventType: config.UpdateType,
		Key:       "test.param",
		Value:     "new-value",
	}

	param.handleConfigChange(event)

	lastVal = param.lastValue.Load()
	assert.NotNil(t, lastVal)
	assert.Equal(t, "default", *lastVal)
}

func TestParamItem_EventTypeFiltering(t *testing.T) {
	manager := config.NewManager()
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}
	param.Init(manager)

	var callbackCalled bool

	callback := func(ctx context.Context, key, oldValue, newValue string) error {
		callbackCalled = true
		return nil
	}

	param.RegisterCallback(callback)

	createEvent := &config.Event{
		EventType: config.CreateType,
		Key:       "test.param",
		Value:     "new-value",
	}

	param.handleConfigChange(createEvent)
	assert.True(t, callbackCalled)

	callbackCalled = false

	updateEvent := &config.Event{
		EventType: config.UpdateType,
		Key:       "test.param",
		Value:     "another-value",
	}

	param.handleConfigChange(updateEvent)
	assert.True(t, callbackCalled)
}

func TestParamItem_NilCallback(t *testing.T) {
	manager := config.NewManager()
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}
	param.Init(manager)

	event := &config.Event{
		EventType: config.UpdateType,
		Key:       "test.param",
		Value:     "new-value",
	}

	assert.NotPanics(t, func() {
		param.handleConfigChange(event)
	})
}

func TestParamItem_CallbackNilManager(t *testing.T) {
	param := &ParamItem{
		Key:          "test.param",
		DefaultValue: "default",
	}

	event := &config.Event{
		EventType: config.UpdateType,
		Key:       "test.param",
		Value:     "new-value",
	}

	assert.NotPanics(t, func() {
		param.handleConfigChange(event)
	})
}
