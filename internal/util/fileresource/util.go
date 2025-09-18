/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package fileresource

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

func download(ctx context.Context, localPath string, downloader storage.ChunkManager, infos []*internalpb.FileResourceInfo) error {
	for _, resource := range infos {
		localResourcePath := path.Join(localPath, fmt.Sprint(resource.GetId()))

		reader, err := downloader.Reader(ctx, resource.GetPath())
		if err != nil {
			return err
		}

		file, err := os.Create(localResourcePath)
		if err != nil {
			return err
		}

		if _, err = io.Copy(file, reader); err != nil {
			return err
		}
	}
	return nil
}

func ParseMode(value string) Mode {
	switch value {
	case "close":
		return CloseMode
	case "sync":
		return SyncMode
	case "ref":
		return RefMode
	default:
		return CloseMode
	}
}

func IsSyncMode(value string) bool {
	return value == "sync"
}
