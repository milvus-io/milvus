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

package validator

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func kv(k, v string) *commonpb.KeyValuePair { return &commonpb.KeyValuePair{Key: k, Value: v} }

func embFn(params ...*commonpb.KeyValuePair) *schemapb.FunctionSchema {
	return &schemapb.FunctionSchema{
		Name:             "emb",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vec"},
		Params:           params,
	}
}

func TestCheckFunctionAlterAllowed(t *testing.T) {
	old := embFn(kv("model_name", "m1"), kv("dim", "8"), kv("url", "http://a"))

	t.Run("change whitelisted param -> ok", func(t *testing.T) {
		newFn := embFn(kv("model_name", "m1"), kv("dim", "8"), kv("url", "http://b"))
		assert.NoError(t, CheckFunctionAlterAllowed(old, newFn))
	})

	t.Run("add whitelisted param -> ok", func(t *testing.T) {
		newFn := embFn(kv("model_name", "m1"), kv("dim", "8"), kv("url", "http://a"), kv("timeout_ms", "1000"))
		assert.NoError(t, CheckFunctionAlterAllowed(old, newFn))
	})

	t.Run("remove whitelisted param -> ok", func(t *testing.T) {
		newFn := embFn(kv("model_name", "m1"), kv("dim", "8"))
		assert.NoError(t, CheckFunctionAlterAllowed(old, newFn))
	})

	t.Run("change truncate/truncation_direction -> rejected (semantic for long inputs)", func(t *testing.T) {
		oldT := embFn(kv("provider", "TEI"), kv("endpoint", "http://tei"))
		newT := embFn(kv("provider", "TEI"), kv("endpoint", "http://tei"), kv("truncate", "true"), kv("truncation_direction", "Left"))
		assert.ErrorContains(t, CheckFunctionAlterAllowed(oldT, newT), "cannot be altered")
	})

	t.Run("change non-whitelisted param -> rejected", func(t *testing.T) {
		newFn := embFn(kv("model_name", "m2"), kv("dim", "8"), kv("url", "http://a"))
		assert.ErrorContains(t, CheckFunctionAlterAllowed(old, newFn), "cannot be altered")
	})

	t.Run("change dim -> rejected", func(t *testing.T) {
		newFn := embFn(kv("model_name", "m1"), kv("dim", "16"), kv("url", "http://a"))
		assert.ErrorContains(t, CheckFunctionAlterAllowed(old, newFn), "cannot be altered")
	})

	t.Run("change endpoint -> rejected (TEI model identity)", func(t *testing.T) {
		oldE := embFn(kv("model_name", "m1"), kv("endpoint", "http://tei-a"))
		newE := embFn(kv("model_name", "m1"), kv("endpoint", "http://tei-b"))
		assert.ErrorContains(t, CheckFunctionAlterAllowed(oldE, newE), "cannot be altered")
	})

	t.Run("duplicate provider key -> rejected (no last-wins bypass)", func(t *testing.T) {
		// last-wins map would see provider unchanged; runtime getProvider is first-wins.
		oldD := embFn(kv("provider", "openai"), kv("url", "http://a"))
		newD := embFn(kv("provider", "evil"), kv("provider", "openai"), kv("url", "http://a"))
		assert.ErrorContains(t, CheckFunctionAlterAllowed(oldD, newD), "duplicate function param key")
	})

	t.Run("change type -> rejected", func(t *testing.T) {
		newFn := embFn(kv("model_name", "m1"), kv("dim", "8"), kv("url", "http://a"))
		newFn.Type = schemapb.FunctionType_BM25
		assert.ErrorContains(t, CheckFunctionAlterAllowed(old, newFn), "type cannot be altered")
	})

	t.Run("change input field -> rejected", func(t *testing.T) {
		newFn := embFn(kv("model_name", "m1"), kv("dim", "8"), kv("url", "http://a"))
		newFn.InputFieldNames = []string{"other"}
		assert.ErrorContains(t, CheckFunctionAlterAllowed(old, newFn), "input fields cannot be altered")
	})

	t.Run("change output field -> rejected", func(t *testing.T) {
		newFn := embFn(kv("model_name", "m1"), kv("dim", "8"), kv("url", "http://a"))
		newFn.OutputFieldNames = []string{"other"}
		assert.ErrorContains(t, CheckFunctionAlterAllowed(old, newFn), "output fields cannot be altered")
	})

	t.Run("bm25 has no alterable params", func(t *testing.T) {
		oldBM := &schemapb.FunctionSchema{
			Name: "b", Type: schemapb.FunctionType_BM25,
			InputFieldNames: []string{"t"}, OutputFieldNames: []string{"s"},
		}
		newBM := &schemapb.FunctionSchema{
			Name: "b", Type: schemapb.FunctionType_BM25,
			InputFieldNames: []string{"t"}, OutputFieldNames: []string{"s"},
			Params: []*commonpb.KeyValuePair{kv("k1", "1.5")},
		}
		assert.ErrorContains(t, CheckFunctionAlterAllowed(oldBM, newBM), "has no alterable params")
	})

	t.Run("minhash has no alterable params (even with no change)", func(t *testing.T) {
		mh := func() *schemapb.FunctionSchema {
			return &schemapb.FunctionSchema{
				Name: "m", Type: schemapb.FunctionType_MinHash,
				InputFieldNames: []string{"t"}, OutputFieldNames: []string{"sig"},
			}
		}
		assert.ErrorContains(t, CheckFunctionAlterAllowed(mh(), mh()), "has no alterable params")
	})
}
