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

package fs

import (
	"fmt"
	"net/url"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storagev2/storage/options"
)

var ErrInvalidFsType = errors.New("invalid fs type")

func BuildFileSystem(uri string) (Fs, error) {
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("build file system with uri %s: %w", uri, err)
	}
	switch parsedURI.Scheme {
	case "file":
		return NewFsFactory().Create(options.LocalFS, parsedURI)
	case "s3":
		return NewFsFactory().Create(options.S3, parsedURI)

	default:
		return nil, fmt.Errorf("build file system with uri %s: %w", uri, ErrInvalidFsType)
	}
}
