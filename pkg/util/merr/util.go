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

package merr

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
)

// Code returns the error code of the given error,
// WARN: DO NOT use this for now
func Code(err error) int32 {
	if err == nil {
		return 0
	}

	cause := errors.Cause(err)
	switch cause := cause.(type) {
	case milvusError:
		return cause.code()

	default:
		if errors.Is(cause, context.Canceled) {
			return CanceledCode
		} else if errors.Is(cause, context.DeadlineExceeded) {
			return TimeoutCode
		} else {
			return errUnexpected.code()
		}
	}
}

// Segment related
func WrapErrSegmentNotFound(id int64, msg ...string) error {
	err := wrapWithField(ErrSegmentNotFound, "segment", id)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

func wrapWithField(err error, name string, value any) error {
	return errors.Wrapf(err, "%s=%v", name, value)
}
