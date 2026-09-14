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

package index

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
)

// fakeIndex counts Delete calls, standing in for the native index that
// indexcgowrapper.CreateIndex hands back as a raw pointer.
type fakeIndex struct {
	indexcgowrapper.CodecIndex
	deleted int
}

func (f *fakeIndex) Delete() error {
	f.deleted++
	return nil
}

func (f *fakeIndex) UpLoad() (*cgopb.IndexStats, error) {
	return &cgopb.IndexStats{}, nil
}

type fakeAnalyze struct {
	deleted int
}

func (f *fakeAnalyze) Delete() error {
	f.deleted++
	return nil
}

func (f *fakeAnalyze) GetResult(size int) (string, int64, []string, []int64, error) {
	return "", 0, nil, nil, nil
}

// processTask skips PostExecute whenever the task context is canceled, and
// PostExecute is the only other place that frees the native index. Reset runs on
// every exit path, so it must do the release or the built index leaks for the
// lifetime of the process.
func TestIndexBuildTaskResetReleasesIndex(t *testing.T) {
	index := &fakeIndex{}
	it := &indexBuildTask{
		ctx:   context.Background(),
		index: index,
	}

	it.Reset()

	assert.Equal(t, 1, index.deleted, "Reset must release the built index")
	assert.Nil(t, it.index)
}

func TestAnalyzeTaskResetReleasesAnalyze(t *testing.T) {
	analyze := &fakeAnalyze{}
	at := &analyzeTask{
		ctx:     context.Background(),
		analyze: analyze,
	}

	at.Reset()

	assert.Equal(t, 1, analyze.deleted, "Reset must release the analyze object")
	assert.Nil(t, at.analyze)
}

// Reset must stay safe after a normal PostExecute, which already released the
// object; the cgo wrappers make Delete idempotent for exactly this reason.
func TestResetAfterReleaseIsSafe(t *testing.T) {
	it := &indexBuildTask{ctx: context.Background()}
	assert.NotPanics(t, it.Reset)

	at := &analyzeTask{ctx: context.Background()}
	assert.NotPanics(t, at.Reset)
}
