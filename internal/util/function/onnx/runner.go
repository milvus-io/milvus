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

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Runner executes tensors for embedding and reranking functions. Manager
// leases own model state.
type Runner struct {
	manager *Manager
}

func NewRunner(manager *Manager) (*Runner, error) {
	if manager == nil {
		return nil, merr.WrapErrParameterInvalidMsg("onnx: manager is nil")
	}
	return &Runner{manager: manager}, nil
}

// Run validates the model signature at the boundary, executes one request,
// and validates the runtime output before returning it to the caller.
func (r *Runner) Run(ctx context.Context, modelPath string, inputs []Tensor) ([]Tensor, error) {
	if r == nil || r.manager == nil {
		return nil, merr.WrapErrServiceInternalMsg("onnx: runner is not initialized")
	}
	lease, err := r.manager.Acquire(ctx, modelPath)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	if err := validateTensors("input", lease.Session().Inputs(), inputs); err != nil {
		return nil, err
	}
	outputs, err := lease.Run(ctx, inputs)
	if err != nil {
		return nil, err
	}
	if err := validateTensors("output", lease.Session().Outputs(), outputs); err != nil {
		return nil, merr.WrapErrFunctionFailed(err, "onnx: runtime returned invalid outputs")
	}
	return outputs, nil
}

func validateTensors(kind string, specs []TensorSpec, tensors []Tensor) error {
	if len(specs) != len(tensors) {
		return tensorContractError(kind, "onnx: %s count mismatch: got %d, expected %d", kind, len(tensors), len(specs))
	}
	seen := make(map[string]struct{}, len(tensors))
	for i, spec := range specs {
		if spec.Name == "" {
			return tensorContractError(kind, "onnx: %s signature %d has an empty name", kind, i)
		}
		if _, ok := seen[spec.Name]; ok {
			return tensorContractError(kind, "onnx: %s signature contains duplicate name %q", kind, spec.Name)
		}
		seen[spec.Name] = struct{}{}
		tensor := tensors[i]
		if tensor.Name != spec.Name {
			return tensorContractError(kind, "onnx: %s %d is %q, expected %q", kind, i, tensor.Name, spec.Name)
		}
		if tensor.DataType != spec.DataType {
			return tensorContractError(kind, "onnx: %s %q has type %s, expected %s", kind, tensor.Name, tensor.DataType, spec.DataType)
		}
		if len(tensor.Shape) != len(spec.Shape) {
			return tensorContractError(kind, "onnx: %s %q has rank %d, expected %d", kind, tensor.Name, len(tensor.Shape), len(spec.Shape))
		}
		for dim, expected := range spec.Shape {
			actual := tensor.Shape[dim]
			if actual < 0 {
				return tensorContractError(kind, "onnx: %s %q has negative dimension %d", kind, tensor.Name, dim)
			}
			if expected >= 0 && actual != expected {
				return tensorContractError(kind, "onnx: %s %q dimension %d is %d, expected %d", kind, tensor.Name, dim, actual, expected)
			}
		}
		if size := tensor.DataType.elementSize(); size > 0 {
			count, err := shapeSize(tensor.Shape)
			if err != nil {
				return err
			}
			expectedBytes := count * int64(size)
			if expectedBytes != int64(len(tensor.Data)) {
				return tensorContractError(kind, "onnx: %s %q has %d payload bytes, expected %d", kind, tensor.Name, len(tensor.Data), expectedBytes)
			}
		}
	}
	return nil
}

func validateSessionSpecs(session Session) error {
	if session == nil {
		return merr.WrapErrServiceInternalMsg("onnx: session is nil")
	}
	for _, kind := range []string{"input", "output"} {
		specs := session.Inputs()
		if kind == "output" {
			specs = session.Outputs()
		}
		seen := make(map[string]struct{}, len(specs))
		for i, spec := range specs {
			if spec.Name == "" {
				return merr.WrapErrFunctionFailedMsg("onnx: runtime %s signature %d has an empty name", kind, i)
			}
			if _, ok := seen[spec.Name]; ok {
				return merr.WrapErrFunctionFailedMsg("onnx: runtime %s signature contains duplicate name %q", kind, spec.Name)
			}
			seen[spec.Name] = struct{}{}
			if spec.DataType.elementSize() == 0 {
				return merr.WrapErrFunctionFailedMsg("onnx: runtime %s %q has an invalid data type", kind, spec.Name)
			}
			for dim, value := range spec.Shape {
				if value < -1 {
					return merr.WrapErrFunctionFailedMsg("onnx: runtime %s %q has invalid dimension %d: %d", kind, spec.Name, dim, value)
				}
			}
		}
	}
	return nil
}

func tensorContractError(kind, format string, args ...any) error {
	if kind == "output" {
		return merr.WrapErrFunctionFailedMsg(format, args...)
	}
	return merr.WrapErrParameterInvalidMsg(format, args...)
}

// FuncBackend adapts a concrete ONNX Runtime loader to Backend.
type FuncBackend struct {
	BackendName    string
	RuntimeVersion string
	LoadFunc       func(context.Context, string) (Session, error)
}

func (b FuncBackend) Name() string    { return b.BackendName }
func (b FuncBackend) Version() string { return b.RuntimeVersion }
func (b FuncBackend) Load(ctx context.Context, path string) (Session, error) {
	if b.LoadFunc == nil {
		return nil, merr.WrapErrServiceInternalMsg("onnx: backend loader is nil")
	}
	if b.BackendName == "" || b.RuntimeVersion == "" {
		return nil, merr.WrapErrParameterInvalidMsg("onnx: backend name and runtime version are required")
	}
	return b.LoadFunc(ctx, path)
}
