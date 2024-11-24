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

package json

import (
	gojson "encoding/json"

	"github.com/bytedance/sonic"
)

var (
	json = sonic.ConfigStd
	// Marshal is exported from bytedance/sonic package.
	Marshal = json.Marshal
	// Unmarshal is exported from bytedance/sonic package.
	Unmarshal = json.Unmarshal
	// MarshalIndent is exported from bytedance/sonic package.
	MarshalIndent = json.MarshalIndent
	// NewDecoder is exported from bytedance/sonic package.
	NewDecoder = json.NewDecoder
	// NewEncoder is exported from bytedance/sonic package.
	NewEncoder = json.NewEncoder
)

type (
	Delim      = gojson.Delim
	Decoder    = gojson.Decoder
	Number     = gojson.Number
	RawMessage = gojson.RawMessage
)
