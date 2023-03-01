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

package server

import (
	"testing"
	"time"

	"github.com/milvus-io/milvus/cdc/server/model/meta"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/cdc/core/mocks"
	"github.com/milvus-io/milvus/cdc/core/model"
	"github.com/milvus-io/milvus/cdc/core/reader"
	"github.com/milvus-io/milvus/cdc/core/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestInvalidOpCDCTask(t *testing.T) {
	factory := NewCDCFactory(func() (reader.CDCReader, error) {
		return &reader.DefaultReader{}, nil
	}, func() (writer.CDCWriter, error) {
		return &writer.DefaultWriter{}, nil
	})
	task := NewCdcTask(factory, &writer.DefaultWriteCallBack{})
	err := <-task.Pause(nil)
	assert.Error(t, err)
	err = <-task.Terminate(nil)
	assert.NoError(t, err)
	err = <-task.Terminate(nil)
	assert.Error(t, err)
	err = <-task.Resume(nil)
	assert.Error(t, err)
}

func TestNormalOpCDCTask(t *testing.T) {
	readerMock := mocks.NewCDCReader(t)
	readerChan := make(chan *model.CDCData)
	var returnChan <-chan *model.CDCData = readerChan
	readerMock.On("StartRead", mock.Anything).Return(returnChan)
	readerMock.On("QuitRead", mock.Anything).Return()
	writerMock := mocks.NewCDCWriter(t)
	writerCall := writerMock.On("Write", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	writerMock.On("Flush", mock.Anything).Return()

	factory := NewCDCFactory(func() (reader.CDCReader, error) {
		return readerMock, nil
	}, func() (writer.CDCWriter, error) {
		return writerMock, nil
	})
	task := NewCdcTask(factory, &writer.DefaultWriteCallBack{})
	err := <-task.Resume(nil)
	assert.NoError(t, err)
	readerChan <- &model.CDCData{}
	err = <-task.Pause(nil)
	assert.NoError(t, err)
	readerMock.AssertCalled(t, "StartRead", mock.Anything)
	readerMock.AssertCalled(t, "QuitRead", mock.Anything)
	task.workingLock.Lock()
	task.workingLock.Unlock()
	writerMock.AssertCalled(t, "Write", mock.Anything, mock.Anything, mock.Anything)
	writerMock.AssertCalled(t, "Flush", mock.Anything)
	err = <-task.Terminate(nil)
	assert.NoError(t, err)

	task = NewCdcTask(factory, &writer.DefaultWriteCallBack{})
	err = <-task.Resume(nil)
	assert.NoError(t, err)
	err = <-task.Terminate(nil)
	assert.NoError(t, err)
	task.workingLock.Lock()
	task.workingLock.Unlock()
	readerMock.AssertNumberOfCalls(t, "StartRead", 2)
	readerMock.AssertNumberOfCalls(t, "QuitRead", 2)
	writerMock.AssertNumberOfCalls(t, "Write", 1)
	writerMock.AssertNumberOfCalls(t, "Flush", 2)

	task = NewCdcTask(factory, &writer.DefaultWriteCallBack{})
	err = <-task.Terminate(nil)
	assert.NoError(t, err)

	writerCall.Unset()
	writerMock.On("Write", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("foo"))
	task = NewCdcTask(factory, &writer.DefaultWriteCallBack{})
	err = <-task.Resume(nil)
	assert.NoError(t, err)
	readerChan <- &model.CDCData{}
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, meta.TaskStatePaused, task.current.Load())
}

func TestFactoryErrorCDCTask(t *testing.T) {
	readerErr := errors.New("foo")
	factory := NewCDCFactory(func() (reader.CDCReader, error) {
		return nil, readerErr
	}, func() (writer.CDCWriter, error) {
		return &writer.DefaultWriter{}, nil
	})
	task := NewCdcTask(factory, &writer.DefaultWriteCallBack{})
	err := <-task.Resume(nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, readerErr)

	writerErr := errors.New("foo")
	factory = NewCDCFactory(func() (reader.CDCReader, error) {
		return &reader.DefaultReader{}, nil
	}, func() (writer.CDCWriter, error) {
		return nil, writerErr
	})
	task = NewCdcTask(factory, &writer.DefaultWriteCallBack{})
	err = <-task.Resume(nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, writerErr)
}

func TestOpErrorCDCTask(t *testing.T) {
	factory := NewCDCFactory(func() (reader.CDCReader, error) {
		return &reader.DefaultReader{}, nil
	}, func() (writer.CDCWriter, error) {
		return &writer.DefaultWriter{}, nil
	})
	task := NewCdcTask(factory, &writer.DefaultWriteCallBack{})
	opErr := errors.New("foo")

	err := <-task.Resume(func() error {
		return opErr
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, opErr)
	op := false
	err = <-task.Resume(func() error {
		op = true
		return nil
	})
	assert.NoError(t, err)
	assert.True(t, op)
	err = <-task.Resume(func() error {
		op = true
		return nil
	})
	assert.NoError(t, err)

	err = <-task.Pause(func() error {
		return opErr
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, opErr)
	op = false
	err = <-task.Pause(func() error {
		op = true
		return nil
	})
	assert.NoError(t, err)
	assert.True(t, op)

	err = <-task.Terminate(func() error {
		return opErr
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, opErr)
	op = false
	err = <-task.Terminate(func() error {
		op = true
		return nil
	})
	assert.NoError(t, err)
	assert.True(t, op)
}
