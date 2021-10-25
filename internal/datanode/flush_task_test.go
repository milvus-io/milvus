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

package datanode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFlushTaskRunner(t *testing.T) {
	task := newFlushTaskRunner(1, nil)
	signal := make(chan struct{})

	saveFlag := false
	nextFlag := false
	processed := make(chan struct{})

	task.init(func(*segmentFlushPack) error {
		saveFlag = true
		return nil
	}, func(pack *segmentFlushPack, i postInjectionFunc) {}, signal)

	go func() {
		<-task.finishSignal
		nextFlag = true
		processed <- struct{}{}
	}()

	assert.False(t, saveFlag)
	assert.False(t, nextFlag)

	task.runFlushInsert(&emptyFlushTask{}, nil, nil, false, nil)
	task.runFlushDel(&emptyFlushTask{}, &DelDataBuf{})

	assert.False(t, saveFlag)
	assert.False(t, nextFlag)

	close(signal)
	<-processed

	assert.True(t, saveFlag)
	assert.True(t, nextFlag)
}

func TestFlushTaskRunner_Injection(t *testing.T) {
	injectCh := make(chan taskInjection, 1)
	task := newFlushTaskRunner(1, injectCh)
	signal := make(chan struct{})

	saveFlag := false
	nextFlag := false
	processed := make(chan struct{})

	injected := make(chan struct{})
	injectOver := make(chan bool)

	injectCh <- taskInjection{
		injected:   injected,
		injectOver: injectOver,
		postInjection: func(pack *segmentFlushPack) {
			t.Log("task injection executed")
			pack.segmentID = 2
		},
	}

	go func() {
		<-injected
		injectOver <- true
	}()

	task.init(func(pack *segmentFlushPack) error {
		assert.EqualValues(t, 2, pack.segmentID)
		saveFlag = true
		return nil
	}, func(pack *segmentFlushPack, i postInjectionFunc) {
		if i != nil {
			i(pack)
		}
	}, signal)

	go func() {
		<-task.finishSignal
		nextFlag = true
		processed <- struct{}{}
	}()

	assert.False(t, saveFlag)
	assert.False(t, nextFlag)

	task.runFlushInsert(&emptyFlushTask{}, nil, nil, false, nil)
	task.runFlushDel(&emptyFlushTask{}, &DelDataBuf{})

	assert.False(t, saveFlag)
	assert.False(t, nextFlag)

	close(signal)
	<-processed

	assert.True(t, saveFlag)
	assert.True(t, nextFlag)

}
