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

package importv2

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type ExecutorSuite struct {
	suite.Suite

	collectionID int64
	executor     Executor
}

func (s *ExecutorSuite) SetupTest() {
	s.collectionID = 1
	//manager := NewTaskManager()
	//s.executor = NewExecutor(manager)
}

func (s *ExecutorSuite) TestRunPreImportTask() {

}

func (s *ExecutorSuite) TestRunImportTask() {

}

func TestExecutor(t *testing.T) {
	suite.Run(t, new(ExecutorSuite))
}
