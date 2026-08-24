// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package cmek

import (
	"testing"

	"github.com/stretchr/testify/suite"
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
	s.runCell(likeCell("fmindex", "FMINDEX"))
}
