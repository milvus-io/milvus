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

package function

import (
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// AnalyzedTextTermBatch contains the message-level unique terms emitted for one
// field. Multi-analyzer row dispatch does not partition the field vocabulary.
// Term order is unspecified; FST construction owns ordering.
type AnalyzedTextTermBatch struct {
	InputFieldID int64
	Terms        [][]byte
}

// TextTermMaterializer lets BM25 runners expose their analyzer output without
// running tokenization a second time.
type TextTermMaterializer interface {
	BatchRunWithTextTerms(inputs ...any) ([]any, []AnalyzedTextTermBatch, error)
	BatchTextTerms(inputs ...any) ([]AnalyzedTextTermBatch, error)
}

type textTermCollectorRunner struct {
	runner        FunctionRunner
	inputFieldIDs []int64
}

// TextTermCollector reruns only fuzzy-enabled BM25 analyzers for writer paths
// that do not pass through WAL materialization. The collected dictionary is
// reset with Drain so one collector can follow segment-writer rotation.
type TextTermCollector struct {
	runners []textTermCollectorRunner
	terms   map[int64]map[string]struct{}
}

// NewTextTermCollector creates analyzer runners for fuzzy-enabled BM25 input
// fields. A schema without such fields returns an inert collector.
func NewTextTermCollector(schema *schemapb.CollectionSchema) (*TextTermCollector, error) {
	collector := &TextTermCollector{
		terms: make(map[int64]map[string]struct{}),
	}
	for _, functionSchema := range schema.GetFunctions() {
		if functionSchema.GetType() != schemapb.FunctionType_BM25 ||
			!typeutil.IsFuzzyEnabledBM25Function(functionSchema) {
			continue
		}
		runner, err := NewFunctionRunner(schema, functionSchema)
		if err != nil {
			collector.Close()
			return nil, err
		}
		if runner == nil {
			collector.Close()
			return nil, merr.WrapErrFunctionFailedMsg("failed to create BM25 runner for text term collection")
		}
		inputFields := runner.GetInputFields()
		inputFieldIDs := make([]int64, len(inputFields))
		for i, field := range inputFields {
			inputFieldIDs[i] = field.GetFieldID()
		}
		if slices.ContainsFunc(collector.runners, func(entry textTermCollectorRunner) bool {
			return slices.Equal(entry.inputFieldIDs, inputFieldIDs)
		}) {
			runner.Close()
			continue
		}
		collector.runners = append(collector.runners, textTermCollectorRunner{
			runner:        runner,
			inputFieldIDs: inputFieldIDs,
		})
	}
	return collector, nil
}

func (c *TextTermCollector) Enabled() bool {
	return c != nil && len(c.runners) > 0
}

// InputFieldIDs returns the unique source fields required by the enabled
// fuzzy BM25 runners. The stable order follows function/schema declaration
// order so callers can build deterministic projected readers.
func (c *TextTermCollector) InputFieldIDs() []int64 {
	if !c.Enabled() {
		return nil
	}
	seen := make(map[int64]struct{})
	result := make([]int64, 0)
	for _, entry := range c.runners {
		for _, fieldID := range entry.inputFieldIDs {
			if _, ok := seen[fieldID]; ok {
				continue
			}
			seen[fieldID] = struct{}{}
			result = append(result, fieldID)
		}
	}
	return result
}

// Collect analyzes one final writer batch. Inputs are field-scoped string
// columns and must include every input of each fuzzy-enabled BM25 function.
func (c *TextTermCollector) Collect(inputs map[int64][]string) error {
	if !c.Enabled() {
		return nil
	}
	for _, entry := range c.runners {
		runnerInputs := make([]any, 0, len(entry.inputFieldIDs))
		for _, fieldID := range entry.inputFieldIDs {
			values, ok := inputs[fieldID]
			if !ok {
				return merr.WrapErrFunctionFailedMsg("text term collector input field %d is missing", fieldID)
			}
			runnerInputs = append(runnerInputs, values)
		}
		materializer, ok := entry.runner.(TextTermMaterializer)
		if !ok {
			return merr.WrapErrFunctionFailedMsg("fuzzy BM25 runner does not support text term collection")
		}
		batches, err := materializer.BatchTextTerms(runnerInputs...)
		if err != nil {
			return err
		}
		for _, batch := range batches {
			termSet, ok := c.terms[batch.InputFieldID]
			if !ok {
				termSet = make(map[string]struct{})
				c.terms[batch.InputFieldID] = termSet
			}
			for _, term := range batch.Terms {
				termSet[string(term)] = struct{}{}
			}
		}
	}
	return nil
}

// Drain freezes the current segment dictionary and starts a new generation.
func (c *TextTermCollector) Drain() map[int64][][]byte {
	if c == nil || len(c.terms) == 0 {
		return nil
	}
	result := make(map[int64][][]byte, len(c.terms))
	for fieldID, terms := range c.terms {
		result[fieldID] = termBytes(terms)
	}
	c.terms = make(map[int64]map[string]struct{})
	return result
}

func (c *TextTermCollector) Close() {
	if c == nil {
		return
	}
	for _, entry := range c.runners {
		entry.runner.Close()
	}
	c.runners = nil
}

func termBytes(terms map[string]struct{}) [][]byte {
	result := make([][]byte, 0, len(terms))
	for term := range terms {
		result = append(result, []byte(term))
	}
	return result
}
