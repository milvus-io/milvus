// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// ClassifyLoonErr marks a producer-classified transient loon failure as
// retryable and everything else as unrecoverable, so callers can pass it
// directly to retry.Do/retry.Handle. Permanent loon errors (access denied,
// corrupt/malformed data), data-integrity errors and unknown errors stop the
// retry loop immediately.
func ClassifyLoonErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrLoonTransient) {
		return err
	}
	return retry.Unrecoverable(err)
}
