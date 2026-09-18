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
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type RawDataV3GrowingSuite struct {
	RawDataV3Suite
}

func (s *RawDataV3GrowingSuite) SetupSuite() {
	s.growingSource = true
	s.setupRawData(3)
}

func TestRawDataV3GrowingSuite(t *testing.T) {
	suite.Run(t, new(RawDataV3GrowingSuite))
}

// Existing production completion logs tie the inspected manifest to the actual
// growing-source writer. Merely enabling the config permits canonical fallback.
func (s *rawDataSuite) assertGrowingSourceFlush(segments []*datapb.SegmentInfo) {
	s.Require().Eventually(func() bool {
		files, err := filepath.Glob(filepath.Join(s.growingLogDir, "*.log"))
		if err != nil {
			return false
		}
		seen := make(map[int64]bool)
		for _, file := range files {
			data, err := os.ReadFile(file)
			if err != nil {
				return false
			}
			for _, line := range bytes.Split(data, []byte{'\n'}) {
				var entry struct {
					Message      string `json:"message"`
					CollectionID int64  `json:"collectionID"`
					SegmentID    int64  `json:"segmentID"`
					TargetOffset int64  `json:"targetOffset"`
					BatchRows    int64  `json:"batchRows"`
					ManifestPath string `json:"manifestPath"`
				}
				if json.Unmarshal(line, &entry) != nil || entry.Message != "growing source sync task done" || entry.BatchRows <= 0 {
					continue
				}
				for _, segment := range segments {
					if entry.CollectionID == segment.GetCollectionID() && entry.SegmentID == segment.GetID() &&
						entry.TargetOffset == segment.GetNumOfRows() && entry.ManifestPath == segment.GetManifestPath() {
						seen[segment.GetID()] = true
					}
				}
			}
		}
		return len(seen) == len(segments)
	}, 10*time.Second, 100*time.Millisecond, "each inspected manifest must come from a nonempty growing-source flush (logs: %s)", s.growingLogDir)
}
