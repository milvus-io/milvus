// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package onnx

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type testBackend struct {
	loads atomic.Int32
	new   func(string) *testSession
}

func (b *testBackend) Name() string    { return "test-onnxruntime" }
func (b *testBackend) Version() string { return "test-1" }
func (b *testBackend) Load(_ context.Context, path string) (Session, error) {
	b.loads.Add(1)
	return b.new(path), nil
}

type testSession struct {
	run       func(context.Context, []Tensor) ([]Tensor, error)
	inputs    []TensorSpec
	outputs   []TensorSpec
	memory    uint64
	close     atomic.Int32
	active    atomic.Int32
	maxActive atomic.Int32
	closeFunc func()
}

func (s *testSession) Inputs() []TensorSpec {
	if s.inputs != nil {
		return s.inputs
	}
	return []TensorSpec{{Name: "input", DataType: DataTypeFloat32, Shape: []int64{-1, 2}}}
}

func (s *testSession) Outputs() []TensorSpec {
	if s.outputs != nil {
		return s.outputs
	}
	return []TensorSpec{{Name: "output", DataType: DataTypeFloat32, Shape: []int64{-1, 2}}}
}

func (s *testSession) MemoryBytes() uint64 {
	if s.memory > 0 {
		return s.memory
	}
	return 16
}

func (s *testSession) Close() error {
	s.close.Add(1)
	if s.closeFunc != nil {
		s.closeFunc()
	}
	return nil
}

func (s *testSession) Run(ctx context.Context, inputs []Tensor) ([]Tensor, error) {
	active := s.active.Add(1)
	for {
		previous := s.maxActive.Load()
		if active <= previous || s.maxActive.CompareAndSwap(previous, active) {
			break
		}
	}
	defer s.active.Add(-1)
	return s.run(ctx, inputs)
}

func newTestTensor(t *testing.T, name string, values ...float32) Tensor {
	t.Helper()
	tensor, err := NewFloat32Tensor(name, []int64{1, 2}, values)
	require.NoError(t, err)
	return tensor
}

func newTestModel(t *testing.T) string {
	t.Helper()
	path := t.TempDir() + "/model.onnx"
	require.NoError(t, os.WriteFile(path, []byte("test model"), 0o600))
	return path
}

func TestRunnerReusesSessionAndValidatesTensorSignature(t *testing.T) {
	backend := &testBackend{new: func(string) *testSession {
		return &testSession{run: func(_ context.Context, inputs []Tensor) ([]Tensor, error) {
			output := append([]byte(nil), inputs[0].Data...)
			for i := 0; i < len(output); i += 4 {
				value := math.Float32frombits(binary.LittleEndian.Uint32(output[i:]))
				binary.LittleEndian.PutUint32(output[i:], math.Float32bits(value+1))
			}
			return []Tensor{{Name: "output", DataType: DataTypeFloat32, Shape: []int64{1, 2}, Data: output}}, nil
		}}
	}}
	manager, err := NewManager(backend, Config{IdleTTL: time.Hour})
	require.NoError(t, err)
	runner, err := NewRunner(manager)
	require.NoError(t, err)
	model := newTestModel(t)

	input := newTestTensor(t, "input", 1, 2)
	outputs, err := runner.Run(context.Background(), model, []Tensor{input})
	require.NoError(t, err)
	require.Equal(t, "output", outputs[0].Name)
	require.Equal(t, float32(2), mathFloat32(outputs[0].Data[0:4]))
	_, err = runner.Run(context.Background(), model, []Tensor{newTestTensor(t, "wrong", 1, 2)})
	require.Error(t, err)
	require.Equal(t, int32(1), backend.loads.Load())

	manager.Close()
	// Close is deferred for active leases and immediate for this idle session.
	require.Eventually(t, func() bool { return manager.SessionCount() == 0 }, time.Second, time.Millisecond)
}

func mathFloat32(data []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(data))
}

func TestManagerSerializedLoadsAndIdleRelease(t *testing.T) {
	backend := &testBackend{new: func(string) *testSession {
		return &testSession{run: func(context.Context, []Tensor) ([]Tensor, error) { return nil, nil }}
	}}
	manager, err := NewManager(backend, Config{IdleTTL: 10 * time.Millisecond})
	require.NoError(t, err)
	model := newTestModel(t)
	const callers = 16
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, acquireErr := manager.Acquire(context.Background(), model)
			if acquireErr == nil {
				lease.Release()
			}
			require.NoError(t, acquireErr)
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), backend.loads.Load())
	require.Eventually(t, func() bool { return manager.SessionCount() == 0 }, time.Second, time.Millisecond)
	manager.Close()
}

func TestManagerAcquireHonorsCancellationWhileAnotherLoadIsInFlight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	backend := &testBackend{new: func(string) *testSession {
		close(started)
		<-release
		return &testSession{run: func(context.Context, []Tensor) ([]Tensor, error) { return nil, nil }}
	}}
	manager, err := NewManager(backend, Config{IdleTTL: time.Hour})
	require.NoError(t, err)
	defer manager.Close()
	model := newTestModel(t)

	firstDone := make(chan error, 1)
	go func() {
		lease, acquireErr := manager.Acquire(context.Background(), model)
		if acquireErr == nil {
			lease.Release()
		}
		firstDone <- acquireErr
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	lease, err := manager.Acquire(ctx, model)
	require.Error(t, err)
	require.Nil(t, lease)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	close(release)
	require.NoError(t, <-firstDone)
}

func TestManagerIdentityChangesWhenModelContentChanges(t *testing.T) {
	backend := &testBackend{new: func(string) *testSession {
		return &testSession{run: func(context.Context, []Tensor) ([]Tensor, error) { return nil, nil }}
	}}
	manager, err := NewManager(backend, Config{IdleTTL: time.Hour, MaxSessions: 2})
	require.NoError(t, err)
	defer manager.Close()
	model := newTestModel(t)
	first, err := manager.Acquire(context.Background(), model)
	require.NoError(t, err)
	firstIdentity := first.Identity()
	first.Release()
	require.NoError(t, os.WriteFile(model, []byte("updated model"), 0o600))
	second, err := manager.Acquire(context.Background(), model)
	require.NoError(t, err)
	require.NotEqual(t, firstIdentity, second.Identity())
	second.Release()
	require.Equal(t, int32(2), backend.loads.Load())
}

func TestManagerBoundsConcurrencyAndMemory(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	backend := &testBackend{new: func(string) *testSession {
		return &testSession{run: func(ctx context.Context, _ []Tensor) ([]Tensor, error) {
			started <- struct{}{}
			select {
			case <-release:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
	}}
	manager, err := NewManager(backend, Config{MaxConcurrency: 1, MaxMemoryBytes: 8})
	require.NoError(t, err)
	model := newTestModel(t)
	lease, err := manager.Acquire(context.Background(), model)
	require.Error(t, err)
	require.Nil(t, lease)
	manager.Close()

	// A second manager demonstrates cancellation while waiting for execution.
	manager, err = NewManager(backend, Config{MaxConcurrency: 1, MaxMemoryBytes: 32})
	require.NoError(t, err)
	defer manager.Close()
	first, err := manager.Acquire(context.Background(), model)
	require.NoError(t, err)
	defer first.Release()
	input := newTestTensor(t, "input", 1, 2)
	go func() { _, _ = first.Run(context.Background(), []Tensor{input}) }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	_, err = first.Run(ctx, []Tensor{input})
	require.Error(t, err)
	close(release)
}

func TestManagerUsesLoadTimeMemoryCharge(t *testing.T) {
	var loaded *testSession
	backend := &testBackend{new: func(string) *testSession {
		loaded = &testSession{memory: 8, run: func(context.Context, []Tensor) ([]Tensor, error) { return nil, nil }}
		return loaded
	}}
	manager, err := NewManager(backend, Config{MaxMemoryBytes: 8, IdleTTL: time.Hour})
	require.NoError(t, err)
	model := newTestModel(t)
	lease, err := manager.Acquire(context.Background(), model)
	require.NoError(t, err)
	require.Equal(t, uint64(8), manager.MemoryBytes())

	loaded.memory = 64
	lease.Release()
	manager.Close()
	require.Eventually(t, func() bool { return manager.MemoryBytes() == 0 }, time.Second, time.Millisecond)
}

func TestRunnerRejectsMalformedRuntimeOutput(t *testing.T) {
	backend := &testBackend{new: func(string) *testSession {
		return &testSession{run: func(context.Context, []Tensor) ([]Tensor, error) {
			return []Tensor{{Name: "unexpected", DataType: DataTypeFloat32, Shape: []int64{1, 2}, Data: make([]byte, 8)}}, nil
		}}
	}}
	manager, err := NewManager(backend, Config{IdleTTL: time.Hour})
	require.NoError(t, err)
	defer manager.Close()
	runner, err := NewRunner(manager)
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), newTestModel(t), []Tensor{newTestTensor(t, "input", 1, 2)})
	require.Error(t, err)
	require.Equal(t, merr.Code(merr.ErrFunctionFailed), merr.Code(err))
}

func TestManagerRejectsUnknownRuntimeDataType(t *testing.T) {
	backend := &testBackend{new: func(string) *testSession {
		return &testSession{
			inputs:  []TensorSpec{{Name: "input", DataType: DataType(255), Shape: []int64{-1, 2}}},
			outputs: []TensorSpec{{Name: "output", DataType: DataTypeFloat32, Shape: []int64{-1, 2}}},
			run:     func(context.Context, []Tensor) ([]Tensor, error) { return nil, nil },
		}
	}}
	manager, err := NewManager(backend, Config{IdleTTL: time.Hour})
	require.NoError(t, err)
	defer manager.Close()

	_, err = manager.Acquire(context.Background(), newTestModel(t))
	require.Error(t, err)
	require.Equal(t, merr.Code(merr.ErrFunctionFailed), merr.Code(err))
}

func TestManagerAcceptsAllSupportedRuntimeDataTypes(t *testing.T) {
	types := []DataType{
		DataTypeFloat32, DataTypeFloat64, DataTypeInt8, DataTypeInt16,
		DataTypeInt32, DataTypeInt64, DataTypeUint8, DataTypeUint16,
		DataTypeUint32, DataTypeUint64, DataTypeBool,
	}
	backend := &testBackend{new: func(string) *testSession {
		specs := make([]TensorSpec, len(types))
		for i, dataType := range types {
			specs[i] = TensorSpec{Name: dataType.String(), DataType: dataType, Shape: []int64{1}}
		}
		return &testSession{
			inputs:  specs,
			outputs: specs,
			run:     func(context.Context, []Tensor) ([]Tensor, error) { return nil, nil },
		}
	}}
	manager, err := NewManager(backend, Config{IdleTTL: time.Hour})
	require.NoError(t, err)
	defer manager.Close()

	lease, err := manager.Acquire(context.Background(), newTestModel(t))
	require.NoError(t, err)
	lease.Release()
}

func TestManagerRejectsOversizedModelBeforeLoad(t *testing.T) {
	backend := &testBackend{new: func(string) *testSession {
		return &testSession{run: func(context.Context, []Tensor) ([]Tensor, error) { return nil, nil }}
	}}
	manager, err := NewManager(backend, Config{MaxModelBytes: 1})
	require.NoError(t, err)
	defer manager.Close()

	_, err = manager.Acquire(context.Background(), newTestModel(t))
	require.Error(t, err)
	require.Equal(t, merr.Code(merr.ErrServiceMemoryLimitExceeded), merr.Code(err))
	require.Equal(t, int32(0), backend.loads.Load())
}

func TestManagerRejectsModelChangedDuringLoad(t *testing.T) {
	var modelPath string
	var loaded *testSession
	backend := &testBackend{new: func(path string) *testSession {
		modelPath = path
		require.NoError(t, os.WriteFile(path, []byte("changed model"), 0o600))
		loaded = &testSession{run: func(context.Context, []Tensor) ([]Tensor, error) { return nil, nil }}
		return loaded
	}}
	manager, err := NewManager(backend, Config{IdleTTL: time.Hour})
	require.NoError(t, err)
	defer manager.Close()

	_, err = manager.Acquire(context.Background(), newTestModel(t))
	require.Error(t, err)
	require.Equal(t, merr.Code(merr.ErrServiceUnavailable), merr.Code(err))
	require.NotEmpty(t, modelPath)
	require.Equal(t, int32(1), loaded.close.Load())
}

func TestRunnerPreservesInferenceFailureAndReleasesLease(t *testing.T) {
	backend := &testBackend{new: func(string) *testSession {
		return &testSession{run: func(context.Context, []Tensor) ([]Tensor, error) {
			return nil, merr.WrapErrServiceUnavailableMsg("runtime is temporarily unavailable")
		}}
	}}
	manager, err := NewManager(backend, Config{IdleTTL: 10 * time.Millisecond})
	require.NoError(t, err)
	runner, err := NewRunner(manager)
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), newTestModel(t), []Tensor{newTestTensor(t, "input", 1, 2)})
	require.Error(t, err)
	require.Equal(t, merr.Code(merr.ErrServiceUnavailable), merr.Code(err))
	require.Eventually(t, func() bool { return manager.SessionCount() == 0 }, time.Second, time.Millisecond)
	manager.Close()
}

func TestRunnerPreservesOutputValidationCause(t *testing.T) {
	backend := &testBackend{new: func(string) *testSession {
		return &testSession{
			outputs: []TensorSpec{{Name: "output", DataType: DataTypeFloat32, Shape: []int64{-1, 2}}},
			run: func(context.Context, []Tensor) ([]Tensor, error) {
				return []Tensor{{Name: "output", DataType: DataTypeFloat32, Shape: []int64{math.MaxInt64, 2}}}, nil
			},
		}
	}}
	manager, err := NewManager(backend, Config{IdleTTL: time.Hour})
	require.NoError(t, err)
	defer manager.Close()
	runner, err := NewRunner(manager)
	require.NoError(t, err)

	_, err = runner.Run(context.Background(), newTestModel(t), []Tensor{newTestTensor(t, "input", 1, 2)})
	require.Error(t, err)
	require.Equal(t, merr.Code(merr.ErrFunctionFailed), merr.Code(err))
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestTensorConstructionRejectsMismatchedValues(t *testing.T) {
	_, err := NewFloat32Tensor("input", []int64{1, 2}, []float32{1})
	require.Error(t, err)
	require.False(t, errors.Is(err, context.Canceled))
}

func TestManagerRejectsExternalTensorData(t *testing.T) {
	backend := &testBackend{new: func(string) *testSession {
		return &testSession{run: func(context.Context, []Tensor) ([]Tensor, error) { return nil, nil }}
	}}
	manager, err := NewManager(backend, Config{IdleTTL: time.Hour})
	require.NoError(t, err)
	defer manager.Close()
	model := t.TempDir() + "/external.onnx"
	// ModelProto.graph.initializer[0].data_location = EXTERNAL.
	modelBytes := []byte{0x3a, 0x04, 0x2a, 0x02, 0x70, 0x01}
	require.NoError(t, os.WriteFile(model, modelBytes, 0o600))

	_, err = manager.Acquire(context.Background(), model)
	require.Error(t, err)
	require.Equal(t, merr.Code(merr.ErrParameterInvalid), merr.Code(err))
	require.Equal(t, int32(0), backend.loads.Load())
}

func TestManagerKeepsPendingCloseWithinLimits(t *testing.T) {
	closeStarted := make(chan struct{})
	closeRelease := make(chan struct{})
	var closeOnce sync.Once
	backend := &testBackend{new: func(string) *testSession {
		return &testSession{closeFunc: func() {
			closeOnce.Do(func() { close(closeStarted) })
			<-closeRelease
		}, run: func(context.Context, []Tensor) ([]Tensor, error) { return nil, nil }}
	}}
	manager, err := NewManager(backend, Config{MaxSessions: 1, MaxMemoryBytes: 16, IdleTTL: time.Hour})
	require.NoError(t, err)
	defer manager.Close()
	firstModel := newTestModel(t)
	first, err := manager.Acquire(context.Background(), firstModel)
	require.NoError(t, err)
	first.Release()

	closeDone := make(chan struct{})
	go func() {
		manager.removeAndClose(first.entry)
		close(closeDone)
	}()
	<-closeStarted
	secondModel := t.TempDir() + "/second.onnx"
	require.NoError(t, os.WriteFile(secondModel, []byte("second model"), 0o600))
	_, err = manager.Acquire(context.Background(), secondModel)
	require.Error(t, err)
	require.Equal(t, merr.Code(merr.ErrServiceResourceInsufficient), merr.Code(err))

	close(closeRelease)
	<-closeDone
	second, err := manager.Acquire(context.Background(), secondModel)
	require.NoError(t, err)
	second.Release()
}

func TestValidateTensorsRejectsPayloadSizeOverflow(t *testing.T) {
	err := validateTensors("input", []TensorSpec{{Name: "input", DataType: DataTypeFloat32, Shape: []int64{-1}}}, []Tensor{{
		Name: "input", DataType: DataTypeFloat32, Shape: []int64{math.MaxInt64},
	}})
	require.Error(t, err)
	require.Equal(t, merr.Code(merr.ErrParameterInvalid), merr.Code(err))
}
