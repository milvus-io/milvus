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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestScalarIndexFMINDEXCampaign(t *testing.T) {
	suite.Run(t, new(ScalarIndexFMINDEXSuite))
}

func (s *ScalarIndexFMINDEXSuite) SetupSuite() {
	s.setup(fmindexCampaign)
}

func (s *ScalarIndexFMINDEXSuite) TearDownSuite() {
	s.tearDown()
}

func (s *ScalarIndexFMINDEXSuite) TestScalarFMINDEXV3() {
	// Native QueryNode initialization is process-wide. A per-suite cost-ratio
	// override depends on which campaign initializes it first. Instead, keep
	// this fixture below the default guard with rare hits and long negative rows.
	cell := likeCell("fmindex", "FMINDEX")
	cell.match = func(i int) bool { return i%500 == 0 }
	cell.data = func(field string, rows int) *schemapb.FieldData {
		values := make([]string, rows)
		tokens, hits := 0, 0
		for i := range values {
			values[i] = strings.Repeat("x", 200)
			if cell.match(i) {
				values[i] = fmt.Sprintf("prefix-needle-%04d", i)
				hits++
			}
			tokens += len(values[i])
		}
		s.Require().Positive(hits)
		// The FMINDEX default SA sample rate is 8 and default cost ratio is .001.
		s.Require().Less(hits*8, tokens/1000, "fixture must be eligible under the default cost guard")
		return stringFieldData(field, values)
	}
	s.runCell(cell)
}
