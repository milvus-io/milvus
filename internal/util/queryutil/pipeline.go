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
	"bytes"
	"context"
	"fmt"

	"go.opentelemetry.io/otel/trace"
)

// Well-known pipeline channel names
const (
	PipelineInput  = "input"
	PipelineOutput = "output"
)

// OpMsg is the message passed between pipeline nodes.
// It uses named channels (string keys) for data flow.
type OpMsg map[string]any

// Node represents a single node in the pipeline.
// It wraps an operator and defines its input/output channels.
type Node struct {
	name    string
	inputs  []string
	outputs []string
	op      Operator
}

// NewNode creates a new pipeline node.
func NewNode(name string, inputs, outputs []string, op Operator) *Node {
	return &Node{
		name:    name,
		inputs:  inputs,
		outputs: outputs,
		op:      op,
	}
}

// unpackInputs extracts input values from the message by channel names.
func (n *Node) unpackInputs(msg OpMsg) ([]any, error) {
	inputs := make([]any, len(n.inputs))
	for i, input := range n.inputs {
		val, ok := msg[input]
		if !ok {
			return nil, fmt.Errorf("node [%s]: input channel '%s' not found", n.name, input)
		}
		inputs[i] = val
	}
	return inputs, nil
}

// packOutputs stores output values into the message by channel names.
func (n *Node) packOutputs(outputs []any, msg OpMsg) error {
	if len(outputs) != len(n.outputs) {
		return fmt.Errorf("node [%s]: output count mismatch, expected %d, got %d",
			n.name, len(n.outputs), len(outputs))
	}
	for i, output := range n.outputs {
		msg[output] = outputs[i]
	}
	return nil
}

// Run executes the node: unpack inputs -> run operator -> pack outputs.
func (n *Node) Run(ctx context.Context, span trace.Span, msg OpMsg) error {
	inputs, err := n.unpackInputs(msg)
	if err != nil {
		return err
	}

	outputs, err := n.op.Run(ctx, span, inputs...)
	if err != nil {
		return fmt.Errorf("node [%s] operator failed: %w", n.name, err)
	}

	return n.packOutputs(outputs, msg)
}

// Name returns the node name.
func (n *Node) Name() string {
	return n.name
}

// Pipeline executes a sequence of nodes, passing data through named channels.
// It can be used at any level: proxy, delegator, or querynode worker.
type Pipeline struct {
	name  string
	nodes []*Node
}

// NewPipeline creates a new pipeline with the given name.
func NewPipeline(name string) *Pipeline {
	return &Pipeline{name: name}
}

// AddNode appends a node to the pipeline.
func (p *Pipeline) AddNode(node *Node) *Pipeline {
	p.nodes = append(p.nodes, node)
	return p
}

// AddNodes appends multiple nodes to the pipeline.
func (p *Pipeline) AddNodes(nodes ...*Node) *Pipeline {
	p.nodes = append(p.nodes, nodes...)
	return p
}

// Run executes the pipeline with the given initial message.
// Returns the final message containing all outputs.
func (p *Pipeline) Run(ctx context.Context, span trace.Span, initialMsg OpMsg) (OpMsg, error) {
	msg := initialMsg
	if msg == nil {
		msg = make(OpMsg)
	}

	for _, node := range p.nodes {
		if err := node.Run(ctx, span, msg); err != nil {
			return nil, fmt.Errorf("pipeline [%s]: %w", p.name, err)
		}
	}

	return msg, nil
}

// GetOutput retrieves the output from the message using the standard output channel.
func (p *Pipeline) GetOutput(msg OpMsg) (any, bool) {
	val, ok := msg[PipelineOutput]
	return val, ok
}

// String returns a human-readable representation of the pipeline.
func (p *Pipeline) String() string {
	buf := bytes.NewBufferString(fmt.Sprintf("Pipeline[%s]:\n", p.name))
	for i, node := range p.nodes {
		buf.WriteString(fmt.Sprintf("  %d. %s: %v -> %v\n", i+1, node.name, node.inputs, node.outputs))
	}
	return buf.String()
}

// PipelineBuilder provides a fluent API for building pipelines.
type PipelineBuilder struct {
	pipeline *Pipeline
}

// NewPipelineBuilder creates a new pipeline builder.
func NewPipelineBuilder(name string) *PipelineBuilder {
	return &PipelineBuilder{
		pipeline: NewPipeline(name),
	}
}

// Add adds a node to the pipeline.
func (b *PipelineBuilder) Add(name string, inputs, outputs []string, op Operator) *PipelineBuilder {
	b.pipeline.AddNode(NewNode(name, inputs, outputs, op))
	return b
}

// Build returns the constructed pipeline.
func (b *PipelineBuilder) Build() *Pipeline {
	return b.pipeline
}
