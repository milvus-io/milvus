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

import "sort"

// AnalyzedTextTermBatch contains the message-level unique terms emitted for one
// field. Multi-analyzer row dispatch does not partition the field vocabulary.
// Terms are sorted by their original analyzer-output bytes.
type AnalyzedTextTermBatch struct {
	InputFieldID int64
	Terms        [][]byte
}

// TextTermMaterializer lets BM25 runners expose their analyzer output without
// running tokenization a second time.
type TextTermMaterializer interface {
	BatchRunWithTextTerms(inputs ...any) ([]any, []AnalyzedTextTermBatch, error)
}

func sortedTermBytes(terms map[string]struct{}) [][]byte {
	values := make([]string, 0, len(terms))
	for term := range terms {
		values = append(values, term)
	}
	sort.Strings(values)

	result := make([][]byte, 0, len(values))
	for _, term := range values {
		result = append(result, []byte(term))
	}
	return result
}
