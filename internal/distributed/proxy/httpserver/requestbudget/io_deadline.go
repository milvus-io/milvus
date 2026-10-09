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

package requestbudget

import (
	"net/http"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ApplyIODeadline installs the same absolute deadline for request body reads
// and response writes. Call it on the original transport writer, before a
// buffering wrapper hides the ResponseController capabilities.
func ApplyIODeadline(w http.ResponseWriter, deadline time.Time) error {
	controller := http.NewResponseController(w)
	if err := controller.SetReadDeadline(deadline); err != nil {
		return merr.WrapErrServiceInternalErr(err, "set REST request body read deadline")
	}
	if err := controller.SetWriteDeadline(deadline); err != nil {
		return merr.WrapErrServiceInternalErr(err, "set REST response write deadline")
	}
	return nil
}
