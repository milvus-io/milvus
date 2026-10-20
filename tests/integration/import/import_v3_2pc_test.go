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
	"testing"

	"github.com/stretchr/testify/suite"
)

// ImportV3TwoPCSuite runs the existing two-phase-commit import contract through
// the V3 gate. It reuses every TwoPCImportSuite case unchanged (only the cluster
// configuration differs), so the same visibility/abort/idempotency assertions
// hold for the V3 reshard chain.
type ImportV3TwoPCSuite struct {
	TwoPCImportSuite
}

func (s *ImportV3TwoPCSuite) SetupSuite() {
	configureImportV3(&s.MiniClusterSuite)
	// TwoPCImportSuite.SetupSuite still selects the DML channel count and the
	// small import scheduling intervals.
	s.TwoPCImportSuite.SetupSuite()
}

func TestImportV3TwoPCSuite(t *testing.T) {
	suite.Run(t, new(ImportV3TwoPCSuite))
}
