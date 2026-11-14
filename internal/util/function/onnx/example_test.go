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
	"fmt"
	"os"
	"time"
)

// exampleSession is a deterministic session used only to show the manager and
// runner lifecycle. A production caller supplies the ONNX Runtime adapter.
type exampleSession struct{}

func (exampleSession) Inputs() []TensorSpec {
	return []TensorSpec{{Name: "input", DataType: DataTypeFloat32, Shape: []int64{1, 2}}}
}

func (exampleSession) Outputs() []TensorSpec {
	return []TensorSpec{{Name: "output", DataType: DataTypeFloat32, Shape: []int64{1, 2}}}
}

func (exampleSession) Run(_ context.Context, inputs []Tensor) ([]Tensor, error) {
	output := inputs[0]
	output.Name = "output"
	return []Tensor{output}, nil
}

func (exampleSession) MemoryBytes() uint64 { return 1 }
func (exampleSession) Close() error        { return nil }

func ExampleRunner() {
	model, err := os.CreateTemp("", "onnx-example-*.onnx")
	if err != nil {
		panic(err)
	}
	modelPath := model.Name()
	defer os.Remove(modelPath)
	if _, err := model.WriteString("example model"); err != nil {
		panic(err)
	}
	if err := model.Close(); err != nil {
		panic(err)
	}

	manager, err := NewManager(FuncBackend{
		BackendName:    "example-runtime",
		RuntimeVersion: "1",
		LoadFunc: func(context.Context, string) (Session, error) {
			return exampleSession{}, nil
		},
	}, Config{IdleTTL: time.Hour})
	if err != nil {
		panic(err)
	}
	defer manager.Close()
	runner, err := NewRunner(manager)
	if err != nil {
		panic(err)
	}
	input, err := NewFloat32Tensor("input", []int64{1, 2}, []float32{1, 2})
	if err != nil {
		panic(err)
	}
	outputs, err := runner.Run(context.Background(), modelPath, []Tensor{input})
	if err != nil {
		panic(err)
	}
	fmt.Println(outputs[0].Name, len(outputs[0].Data))
	// Output: output 8
}
