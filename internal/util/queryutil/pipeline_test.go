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

package queryutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

// doubleOp doubles an int64 input.
func doubleOp() Operator {
	return NewLambdaOperator("double", func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
		return []any{inputs[0].(int64) * 2}, nil
	})
}

// addOp adds two int64 inputs.
func addOp() Operator {
	return NewLambdaOperator("add", func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
		return []any{inputs[0].(int64) + inputs[1].(int64)}, nil
	})
}

// failOp always returns an error.
func failOp() Operator {
	return NewLambdaOperator("fail", func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
		return nil, fmt.Errorf("intentional failure")
	})
}

// splitOp splits one input into two outputs.
func splitOp() Operator {
	return NewLambdaOperator("split", func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
		v := inputs[0].(int64)
		return []any{v, v * 10}, nil
	})
}

func TestNode_Run_Basic(t *testing.T) {
	node := NewNode("double", []string{"input"}, []string{"output"}, doubleOp())
	msg := OpMsg{"input": int64(5)}
	err := node.Run(context.Background(), nil, msg)
	require.NoError(t, err)
	assert.Equal(t, int64(10), msg["output"])
}

func TestNode_Run_MultiInput(t *testing.T) {
	node := NewNode("add", []string{"a", "b"}, []string{"sum"}, addOp())
	msg := OpMsg{"a": int64(3), "b": int64(7)}
	err := node.Run(context.Background(), nil, msg)
	require.NoError(t, err)
	assert.Equal(t, int64(10), msg["sum"])
}

func TestNode_Run_MultiOutput(t *testing.T) {
	node := NewNode("split", []string{"input"}, []string{"lo", "hi"}, splitOp())
	msg := OpMsg{"input": int64(5)}
	err := node.Run(context.Background(), nil, msg)
	require.NoError(t, err)
	assert.Equal(t, int64(5), msg["lo"])
	assert.Equal(t, int64(50), msg["hi"])
}

func TestNode_Run_MissingInput(t *testing.T) {
	node := NewNode("double", []string{"missing_channel"}, []string{"output"}, doubleOp())
	msg := OpMsg{"input": int64(5)}
	err := node.Run(context.Background(), nil, msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing_channel")
	assert.Contains(t, err.Error(), "not found")
}

func TestNode_Run_OutputCountMismatch(t *testing.T) {
	// doubleOp returns 1 output, but node expects 2 output channels
	node := NewNode("bad", []string{"input"}, []string{"out1", "out2"}, doubleOp())
	msg := OpMsg{"input": int64(5)}
	err := node.Run(context.Background(), nil, msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "output count mismatch")
}

func TestNode_Run_OperatorError(t *testing.T) {
	node := NewNode("fail", []string{"input"}, []string{"output"}, failOp())
	msg := OpMsg{"input": int64(5)}
	err := node.Run(context.Background(), nil, msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "operator failed")
	assert.Contains(t, err.Error(), "intentional failure")
}

func TestNode_Name(t *testing.T) {
	node := NewNode("my-node", nil, nil, doubleOp())
	assert.Equal(t, "my-node", node.Name())
}

func TestPipeline_Run_SingleNode(t *testing.T) {
	p := NewPipeline("test")
	p.AddNode(NewNode("double", []string{PipelineInput}, []string{PipelineOutput}, doubleOp()))

	msg, err := p.Run(context.Background(), nil, OpMsg{PipelineInput: int64(7)})
	require.NoError(t, err)
	val, ok := p.GetOutput(msg)
	assert.True(t, ok)
	assert.Equal(t, int64(14), val)
}

func TestPipeline_Run_ChainedNodes(t *testing.T) {
	p := NewPipeline("chain")
	p.AddNode(NewNode("double1", []string{PipelineInput}, []string{"mid"}, doubleOp()))
	p.AddNode(NewNode("double2", []string{"mid"}, []string{PipelineOutput}, doubleOp()))

	msg, err := p.Run(context.Background(), nil, OpMsg{PipelineInput: int64(3)})
	require.NoError(t, err)
	val, ok := p.GetOutput(msg)
	assert.True(t, ok)
	assert.Equal(t, int64(12), val) // 3 * 2 * 2
}

func TestPipeline_Run_NilInitialMsg(t *testing.T) {
	p := NewPipeline("empty")
	msg, err := p.Run(context.Background(), nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, msg)
}

func TestPipeline_Run_NodeError(t *testing.T) {
	p := NewPipeline("fail-pipe")
	p.AddNode(NewNode("fail", []string{PipelineInput}, []string{PipelineOutput}, failOp()))

	_, err := p.Run(context.Background(), nil, OpMsg{PipelineInput: int64(1)})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pipeline [fail-pipe]")
}

func TestPipeline_GetOutput_Missing(t *testing.T) {
	p := NewPipeline("test")
	msg := OpMsg{"something_else": 42}
	_, ok := p.GetOutput(msg)
	assert.False(t, ok)
}

func TestPipeline_AddNodes(t *testing.T) {
	p := NewPipeline("multi")
	n1 := NewNode("n1", []string{PipelineInput}, []string{"mid"}, doubleOp())
	n2 := NewNode("n2", []string{"mid"}, []string{PipelineOutput}, doubleOp())
	p.AddNodes(n1, n2)

	msg, err := p.Run(context.Background(), nil, OpMsg{PipelineInput: int64(5)})
	require.NoError(t, err)
	val, ok := p.GetOutput(msg)
	assert.True(t, ok)
	assert.Equal(t, int64(20), val)
}

func TestPipeline_String(t *testing.T) {
	p := NewPipeline("debug")
	p.AddNode(NewNode("step1", []string{"in"}, []string{"mid"}, doubleOp()))
	p.AddNode(NewNode("step2", []string{"mid"}, []string{"out"}, doubleOp()))

	s := p.String()
	assert.Contains(t, s, "Pipeline[debug]")
	assert.Contains(t, s, "step1")
	assert.Contains(t, s, "step2")
}

func TestPipelineBuilder(t *testing.T) {
	p := NewPipelineBuilder("builder-test").
		Add("double", []string{PipelineInput}, []string{"mid"}, doubleOp()).
		Add("double2", []string{"mid"}, []string{PipelineOutput}, doubleOp()).
		Build()

	msg, err := p.Run(context.Background(), nil, OpMsg{PipelineInput: int64(4)})
	require.NoError(t, err)
	val, ok := p.GetOutput(msg)
	assert.True(t, ok)
	assert.Equal(t, int64(16), val)
}

func TestLambdaOperator(t *testing.T) {
	op := NewLambdaOperator("test-lambda", func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
		return []any{inputs[0].(string) + "-done"}, nil
	})
	assert.Equal(t, "test-lambda", op.Name())

	outputs, err := op.Run(context.Background(), nil, "hello")
	require.NoError(t, err)
	assert.Equal(t, "hello-done", outputs[0])
}
