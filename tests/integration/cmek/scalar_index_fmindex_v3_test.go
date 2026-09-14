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

package cmek

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestScalarIndexFMINDEXCampaign(t *testing.T) {
	suite.Run(t, new(ScalarIndexFMINDEXSuite))
}

func (s *ScalarIndexFMINDEXSuite) SetupSuite() {
	// Make the query oracle exercise FMINDEX even for this small fixture. The
	// default cost ratio intentionally falls back to a raw scan for the
	// fixture's match cardinality, which would make the oracle weaker than it
	// appears.
	s.WithMilvusConfig("queryNode.fmindexCostRatio", "1.0")
	s.setup(fmindexCampaign)
}

func (s *ScalarIndexFMINDEXSuite) TearDownSuite() {
	s.tearDown()
}

func (s *ScalarIndexFMINDEXSuite) TestScalarFMINDEXV3() {
	s.runCell(likeCell("fmindex", "FMINDEX"))
}
