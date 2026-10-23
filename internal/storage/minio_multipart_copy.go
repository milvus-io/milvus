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

package storage

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// Match milvus-backup's multipart layout and per-file parallelism. The DataNode
// permit still covers a whole file, so each active file can issue ten part requests.
const (
	minioCopyPartSize        = 10 * 1024 * 1024
	minioCopyMaxParts        = 10000
	minioCopyMaxObjectSize   = 5 * 1024 * 1024 * 1024 * 1024
	minioCopyPartConcurrency = 10
)

// copyObjectMultipart reuses the caller's Stat and client (credentials, endpoint,
// transport). Each invocation owns its upload ID; concurrent copies to the same
// immutable destination never abort or delete another invocation's output.
func (s *MinioObjectStorage) copyObjectMultipart(ctx context.Context, dst minio.CopyDestOptions, src minio.CopySrcOptions, info minio.ObjectInfo) error {
	if info.Size <= 0 || info.Size > minioCopyMaxObjectSize {
		return merr.WrapErrDataIntegrityMsg("invalid multipart source size: %d", info.Size)
	}
	partSize := max(int64(minioCopyPartSize), (info.Size+minioCopyMaxParts-1)/minioCopyMaxParts)
	parts := make([]minio.CompletePart, (info.Size+partSize-1)/partSize)
	core := minio.Core{Client: s.Client}
	uploadID, err := core.NewMultipartUpload(ctx, dst.Bucket, dst.Object, minio.PutObjectOptions{
		UserMetadata: info.UserMetadata, UserTags: info.UserTags, ContentType: info.ContentType,
	})
	if err != nil {
		return err
	}
	if uploadID == "" {
		return merr.WrapErrIoFailedMsg("multipart copy returned an empty upload ID")
	}
	completed := false
	defer func() {
		if completed {
			return
		}
		// Wait below joins all part calls before aborting. Cancellation of the task
		// must not cancel cleanup. Only this upload is removed, never the target key.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		err := retry.Do(cleanupCtx, func() error {
			err := core.AbortMultipartUpload(cleanupCtx, dst.Bucket, dst.Object, uploadID)
			if minio.ToErrorResponse(err).Code == "NoSuchUpload" {
				return nil
			}
			return err
		}, retry.Attempts(3), retry.RetryErr(retryMultipartCopyError))
		if err != nil {
			mlog.Warn(cleanupCtx, "failed to abort multipart copy", mlog.String("bucket", dst.Bucket), mlog.String("object", dst.Object), mlog.String("uploadID", uploadID), mlog.Err(err))
		}
	}()
	headers := make(map[string]string)
	if info.ETag != "" {
		headers["x-amz-copy-source-if-match"] = `"` + strings.Trim(info.ETag, `"`) + `"`
	}
	group, partCtx := errgroup.WithContext(ctx)
	group.SetLimit(minioCopyPartConcurrency)
	for i := range parts {
		if partCtx.Err() != nil {
			break
		}
		group.Go(func() error {
			offset := int64(i) * partSize
			return retry.Do(partCtx, func() error {
				part, err := core.CopyObjectPart(partCtx, src.Bucket, src.Object, dst.Bucket, dst.Object, uploadID,
					i+1, offset, min(partSize, info.Size-offset), headers)
				if err != nil {
					return err
				}
				// minio-go can decode an HTTP 200 <Error> as an empty CopyPartResult.
				// Never complete an upload or count bytes for an unconfirmed part.
				if part.ETag == "" {
					return merr.WrapErrIoFailedMsg("multipart copy part %d returned an empty ETag", i+1)
				}
				parts[i] = part
				return nil
			}, retry.Attempts(10), retry.Sleep(200*time.Millisecond), retry.MaxSleepTime(3*time.Second), retry.RetryErr(retryMultipartCopyError))
		})
	}
	err = group.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	// Indexed slots retain part-number order regardless of completion order.
	// Complete failures are ambiguous: fail closed and best-effort abort; do not
	// infer success from destination size or replay the entire object here.
	result, err := core.CompleteMultipartUpload(ctx, dst.Bucket, dst.Object, uploadID, parts, minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	if result.ETag == "" {
		return merr.WrapErrIoFailedMsg("multipart copy completion returned an empty ETag")
	}
	completed = true
	return nil
}

// Classify raw SDK responses before mapObjectStorageError translates them.
// Retrying the same upload/part/range replaces that part safely. Permanent 4xx
// (including a changed source ETag or a lost upload) cannot heal within this upload.
func retryMultipartCopyError(err error) bool {
	if merr.IsCanceledOrTimeout(err) {
		return false
	}
	response := minio.ToErrorResponse(err)
	switch response.Code {
	case "NoSuchUpload", "NoSuchKey", "NoSuchBucket", "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidArgument", "InvalidRequest", "InvalidRange", "PreconditionFailed":
		return false
	case "RequestError", "RequestTimeout", "Throttling", "ThrottlingException", "RequestLimitExceeded", "RequestThrottled", "InternalError", "ExpiredToken", "ExpiredTokenException", "SlowDown":
		return true
	}
	return response.StatusCode == 0 || response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError
}
