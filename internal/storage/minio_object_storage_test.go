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
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestMinioObjectStoragePutObjectOptions(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.MinioCfg.DisableAWSChunkedEncoding.Key, "false")
	t.Cleanup(func() {
		params.Reset(params.MinioCfg.DisableAWSChunkedEncoding.Key)
	})

	storage := &MinioObjectStorage{}
	opts := storage.putObjectOptions()
	assert.False(t, opts.DisableContentSha256)
	assert.False(t, opts.SendContentMd5)

	params.Save(params.MinioCfg.DisableAWSChunkedEncoding.Key, "true")
	opts = storage.putObjectOptions()
	assert.True(t, opts.DisableContentSha256)
	assert.False(t, opts.SendContentMd5)
}

func TestMinioObjectStorageCopyObjectCrossBucket(t *testing.T) {
	params := paramtable.Get()
	key := params.MinioCfg.MultipartCopyThreshold.Key
	original := params.MinioCfg.MultipartCopyThreshold.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save(key, original)) })

	for _, tc := range []struct {
		name          string
		size          int64
		threshold     string
		provider      string
		address       string
		useIAM        bool
		sameBucket    bool
		wantMultipart bool
		statErr       error
		copyErr       error
	}{
		{name: "empty"},
		{name: "tiny_threshold", size: minioCopyPartSize - 1, threshold: "1"},
		{name: "minimum_multipart", size: minioCopyPartSize, threshold: "1", wantMultipart: true},
		{name: "below_default", size: 1024*1024*1024 - 1},
		{name: "at_default", size: 1024 * 1024 * 1024, wantMultipart: true},
		{name: "aliyun_two_gb", size: 2_000_000_000, provider: objectstorage.CloudProviderAliyun, wantMultipart: true},
		{name: "aws_two_gb", size: 2_000_000_000, provider: objectstorage.CloudProviderAWS, wantMultipart: true},
		{name: "same_bucket_at_default", size: 1024 * 1024 * 1024, sameBucket: true, wantMultipart: true},
		{name: "lower_threshold", size: 500_000_001, threshold: "500000000", wantMultipart: true},
		{name: "raise_threshold", size: 2_000_000_000, threshold: "2000000000", wantMultipart: true},
		{name: "legacy_threshold", size: 5 * 1024 * 1024 * 1024, threshold: "5368709120", wantMultipart: true},
		{name: "large_threshold_does_not_fall_back", size: 2_000_000_000, threshold: "10737418240"},
		{name: "large_threshold_at_protocol_limit", size: 5 * 1024 * 1024 * 1024, threshold: "10737418240"},
		{name: "large_threshold_above_protocol_limit", size: 5*1024*1024*1024 + 1, threshold: "10737418240", wantMultipart: true},
		{name: "gcp_exemption", size: 6 * 1024 * 1024 * 1024, provider: objectstorage.CloudProviderGCP},
		{name: "gcp_large_threshold", size: 6 * 1024 * 1024 * 1024, threshold: "10737418240", provider: objectstorage.CloudProviderGCP},
		{name: "gcp_custom_threshold", size: 2_000_000_000, threshold: "1", provider: objectstorage.CloudProviderGCP},
		{name: "gcp_inferred_default", size: 2_000_000_000, address: "storage.googleapis.com"},
		{name: "gcp_inferred_aws", size: 2_000_000_000, provider: objectstorage.CloudProviderAWS, address: "storage.googleapis.com:443"},
		{name: "gcp_inferred_iam", size: 2_000_000_000, provider: objectstorage.CloudProviderAWS, address: "storage.googleapis.com", useIAM: true},
		{name: "gcp_inferred_custom_threshold", size: 2_000_000_000, threshold: "1", address: "storage.googleapis.com"},
		{name: "gcp_inferred_above_five_gib", size: 6 * 1024 * 1024 * 1024, address: "storage.googleapis.com"},
		{name: "aliyun_inferred", size: 2_000_000_000, provider: objectstorage.CloudProviderAWS, address: "oss-cn-hangzhou.aliyuncs.com", wantMultipart: true},
		{name: "explicit_aliyun_over_gcp_endpoint", size: 2_000_000_000, provider: objectstorage.CloudProviderAliyun, address: "storage.googleapis.com", wantMultipart: true},
		{name: "stat_failure", size: 2_000_000_000, statErr: context.Canceled},
		{name: "single_copy_failure", size: 10, copyErr: context.Canceled},
		{name: "multipart_failure", size: 2_000_000_000, wantMultipart: true, copyErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			threshold := tc.threshold
			if threshold == "" {
				threshold = params.MinioCfg.MultipartCopyThreshold.DefaultValue
			}
			require.NoError(t, params.Save(key, threshold))
			dstBucket := "dst-bucket"
			if tc.sameBucket {
				dstBucket = "src-bucket"
			}
			mockStat := mockey.Mock((*minio.Client).StatObject).Return(minio.ObjectInfo{Size: tc.size}, tc.statErr).Build()
			defer mockStat.UnPatch()
			var copyCalls, multipartCalls int
			var gotDst minio.CopyDestOptions
			var gotSrc minio.CopySrcOptions
			mockCopy := mockey.Mock(minio.Core.CopyObject).To(
				func(_ minio.Core, _ context.Context, srcBucket, srcObject, dstBucket, dstObject string, _ map[string]string, _ minio.CopySrcOptions, _ minio.PutObjectOptions) (minio.ObjectInfo, error) {
					copyCalls++
					gotDst = minio.CopyDestOptions{Bucket: dstBucket, Object: dstObject}
					gotSrc = minio.CopySrcOptions{Bucket: srcBucket, Object: srcObject}
					return minio.ObjectInfo{ETag: "copied"}, tc.copyErr
				}).Build()
			defer mockCopy.UnPatch()
			mockMultipart := mockey.Mock((*MinioObjectStorage).copyObjectMultipart).To(
				func(_ *MinioObjectStorage, _ context.Context, dst minio.CopyDestOptions, src minio.CopySrcOptions, info minio.ObjectInfo, _ int64) error {
					multipartCalls++
					require.Equal(t, tc.size, info.Size)
					gotDst, gotSrc = dst, src
					return tc.copyErr
				}).Build()
			defer mockMultipart.UnPatch()

			address := tc.address
			if address == "" {
				address = "localhost:9000"
			}
			config := objectstorage.Config{
				Address: address, CloudProvider: tc.provider, UseIAM: tc.useIAM,
				BucketName: "src-bucket", SkipBucketCheck: true,
				AccessKeyID: "access-key", SecretAccessKeyID: "secret-key",
			}
			originalConfig := config
			objectStorage, err := newMinioObjectStorageWithConfig(context.Background(), &config)
			require.NoError(t, err)
			assert.Equal(t, originalConfig, config, "client construction must not change shared configuration")
			err = objectStorage.CopyObjectCrossBucket(context.Background(), "src-bucket", "src-object", dstBucket, "dst-object")
			if tc.statErr != nil {
				require.ErrorIs(t, err, tc.statErr)
				assert.Zero(t, copyCalls+multipartCalls)
				return
			}
			if tc.copyErr != nil {
				require.ErrorIs(t, err, tc.copyErr)
			} else {
				require.NoError(t, err)
			}
			if tc.wantMultipart {
				assert.Equal(t, 1, multipartCalls)
				assert.Zero(t, copyCalls)
			} else {
				assert.Equal(t, 1, copyCalls)
				assert.Zero(t, multipartCalls)
			}
			assert.Equal(t, "src-bucket", gotSrc.Bucket)
			assert.Equal(t, "src-object", gotSrc.Object)
			assert.Equal(t, dstBucket, gotDst.Bucket)
			assert.Equal(t, "dst-object", gotDst.Object)
		})
	}
}

// Keep Core.CopyObject real so this covers SDK XML decoding rather than mock results.
func TestMinioObjectStorageSingleCopyResponse(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		size       int64
		wantError  bool
	}{
		{name: "body_etag_without_header", size: 7, body: `<CopyObjectResult><ETag>"copied"</ETag></CopyObjectResult>`},
		{name: "empty_object", body: `<CopyObjectResult><ETag>"empty-object"</ETag></CopyObjectResult>`},
		{name: "embedded_error", body: `<Error><Code>InternalError</Code><Message>copy failed</Message></Error>`, wantError: true},
		{name: "missing_etag", body: `<CopyObjectResult/>`, wantError: true},
		{name: "malformed_xml", body: `<CopyObjectResult><ETag>`, wantError: true},
		{name: "empty_body", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var puts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodHead:
					assert.Equal(t, "/src-bucket/source", r.URL.Path)
					w.Header().Set("Content-Length", strconv.FormatInt(tc.size, 10))
					w.Header().Set("ETag", `"source-etag"`)
					w.Header().Set("Last-Modified", "Wed, 23 Sep 2026 00:00:00 GMT")
				case http.MethodPut:
					puts.Add(1)
					assert.Equal(t, "/dst-bucket/target", r.URL.Path)
					assert.Equal(t, "src-bucket/source", r.Header.Get("X-Amz-Copy-Source"))
					w.Header().Set("Content-Type", "application/xml")
					// Error responses must fail even if a proxy adds an ETag header.
					if tc.wantError {
						w.Header().Set("ETag", `"unconfirmed"`)
					}
					fmt.Fprint(w, tc.body)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{
				Creds: credentials.NewStaticV4("access-key", "secret-key", ""), Region: "us-east-1", BucketLookup: minio.BucketLookupPath,
			})
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			size, err := (&MinioObjectStorage{Client: client}).CopyObjectCrossBucketAndGetSize(ctx, "src-bucket", "source", "dst-bucket", "target")
			require.EqualValues(t, 1, puts.Load(), "must reach the real copy response")
			if tc.wantError {
				require.ErrorIs(t, err, merr.ErrIoFailed)
				require.Zero(t, size, "unconfirmed copies must not count bytes")
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.size, size)
			}
		})
	}
}

// Exercise real SDK requests, including its HTTP 200 error decoding, without
// allocating or transferring the source object's bytes.
func TestMinioObjectStorageCopyObjectCrossBucketMultipartRequests(t *testing.T) {
	params := paramtable.Get()
	key := params.MinioCfg.MultipartCopyThreshold.Key
	original := params.MinioCfg.MultipartCopyThreshold.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save(key, original)) })
	require.NoError(t, params.Save(key, "524288000"))
	const size int64 = 500*1024*1024 + 3
	for _, scenario := range []string{"success", "part_access_denied", "empty_part_etag", "complete_error", "empty_complete_etag"} {
		t.Run(scenario, func(t *testing.T) {
			var mu sync.Mutex
			ranges := make(map[int]string)
			attempts := make(map[int]int)
			var completed struct {
				Parts []minio.CompletePart `xml:"Part"`
			}
			var heads, active, peak int
			var initiated, finished, aborted bool
			firstWave := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				query := r.URL.Query()
				mu.Lock()
				switch {
				case r.Method == http.MethodHead && r.URL.Path == "/src-bucket/src-object":
					heads++
					w.Header().Set("Content-Length", fmt.Sprint(size))
					w.Header().Set("ETag", `"source-etag"`)
					w.Header().Set("X-Amz-Meta-Test", "preserved")
					w.Header().Set("Last-Modified", "Wed, 23 Sep 2026 00:00:00 GMT")
					mu.Unlock()
				case r.Method == http.MethodPost && query.Has("uploads"):
					assert.Equal(t, "/dst-bucket/dst-object", r.URL.Path)
					assert.Equal(t, "preserved", r.Header.Get("X-Amz-Meta-Test"))
					initiated = true
					mu.Unlock()
					fmt.Fprint(w, `<InitiateMultipartUploadResult><Bucket>dst-bucket</Bucket><Key>dst-object</Key><UploadId>copy-upload</UploadId></InitiateMultipartUploadResult>`)
				case r.Method == http.MethodPut && query.Get("uploadId") == "copy-upload":
					assert.True(t, initiated)
					assert.Equal(t, "/dst-bucket/dst-object", r.URL.Path)
					assert.Equal(t, "src-bucket/src-object", r.Header.Get("X-Amz-Copy-Source"))
					assert.Equal(t, `"source-etag"`, r.Header.Get("X-Amz-Copy-Source-If-Match"))
					assert.Equal(t, int64(0), r.ContentLength)
					part, err := strconv.Atoi(query.Get("partNumber"))
					assert.NoError(t, err)
					byteRange := r.Header.Get("X-Amz-Copy-Source-Range")
					if previous, ok := ranges[part]; ok {
						assert.Equal(t, previous, byteRange)
					}
					ranges[part] = byteRange
					attempts[part]++
					attempt := attempts[part]
					active++
					peak = max(peak, active)
					if active == minioCopyPartConcurrency && peak == active {
						select {
						case <-firstWave:
						default:
							close(firstWave)
						}
					}
					mu.Unlock()
					// A serial implementation cannot release this barrier.
					select {
					case <-firstWave:
					case <-r.Context().Done():
					}
					defer func() { mu.Lock(); active--; mu.Unlock() }()
					if scenario == "part_access_denied" && part == 2 {
						w.WriteHeader(http.StatusForbidden)
						fmt.Fprint(w, `<Error><Code>AccessDenied</Code><Message>part copy denied</Message></Error>`)
						return
					}
					if scenario == "empty_part_etag" && part == 2 && attempt == 1 {
						fmt.Fprint(w, `<Error><Code>InternalError</Code><Message>copy failed</Message></Error>`)
						return
					}
					fmt.Fprintf(w, `<CopyPartResult><LastModified>2026-09-23T00:00:00Z</LastModified><ETag>"part-%d"</ETag></CopyPartResult>`, part)
				case r.Method == http.MethodPost && query.Get("uploadId") == "copy-upload":
					assert.NoError(t, xml.NewDecoder(r.Body).Decode(&completed))
					finished = true
					mu.Unlock()
					switch scenario {
					case "complete_error":
						fmt.Fprint(w, `<Error><Code>AccessDenied</Code><Message>complete denied</Message></Error>`)
					case "empty_complete_etag":
						fmt.Fprint(w, `<CompleteMultipartUploadResult><Bucket>dst-bucket</Bucket><Key>dst-object</Key></CompleteMultipartUploadResult>`)
					default:
						fmt.Fprint(w, `<CompleteMultipartUploadResult><Bucket>dst-bucket</Bucket><Key>dst-object</Key><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`)
					}
				case r.Method == http.MethodDelete && query.Get("uploadId") == "copy-upload":
					aborted = true
					mu.Unlock()
					w.WriteHeader(http.StatusNoContent)
				default:
					mu.Unlock()
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{
				Creds: credentials.NewStaticV4("access-key", "secret-key", ""), Region: "us-east-1", BucketLookup: minio.BucketLookupPath,
			})
			require.NoError(t, err)
			objectStorage := &MinioObjectStorage{Client: client}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			copied, err := objectStorage.CopyObjectCrossBucketAndGetSize(ctx, "src-bucket", "src-object", "dst-bucket", "dst-object")
			// Wait for HTTP handlers as well as client calls before inspecting counters.
			server.Close()
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, 1, heads)
			assert.Equal(t, minioCopyPartConcurrency, peak)
			if scenario == "part_access_denied" || scenario == "complete_error" || scenario == "empty_complete_etag" {
				require.Error(t, err)
				if scenario != "empty_complete_etag" {
					assert.ErrorIs(t, err, merr.ErrIoPermissionDenied)
				}
				assert.Zero(t, copied)
				assert.True(t, aborted)
				if scenario == "part_access_denied" {
					assert.False(t, finished)
					assert.Equal(t, 1, attempts[2])
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, size, copied)
			assert.True(t, finished)
			assert.False(t, aborted)
			require.Len(t, completed.Parts, 51)
			for i, part := range completed.Parts {
				assert.Equal(t, i+1, part.PartNumber)
				assert.Equal(t, fmt.Sprintf("part-%d", i+1), strings.Trim(part.ETag, `"`))
				offset := int64(i) * minioCopyPartSize
				assert.Equal(t, fmt.Sprintf("bytes=%d-%d", offset, min(offset+minioCopyPartSize, size)-1), ranges[i+1])
				wantAttempts := 1
				if scenario == "empty_part_etag" && i+1 == 2 {
					wantAttempts = 2
				}
				assert.Equal(t, wantAttempts, attempts[i+1])
			}
		})
	}
}

func TestMinioMultipartCopyLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		size                     int64
		initErr                  error
		emptyID, cancelAfterInit bool
		partErr                  error
		failures                 int
		abortErr                 error
		wantAttempts, wantAborts int
	}{
		{name: "zero", size: 0},
		{name: "negative", size: -1},
		{name: "oversized", size: 1<<63 - 1},
		{name: "init_failure", size: 1, initErr: context.DeadlineExceeded},
		{name: "empty_upload_id", size: 1, emptyID: true},
		{name: "canceled_after_init", size: 1, cancelAfterInit: true, wantAborts: 1},
		{name: "success", size: minioCopyPartSize + 1, wantAttempts: 2},
		{name: "max_size", size: minioCopyMaxObjectSize, wantAttempts: minioCopyMaxParts},
		{name: "transient_part", size: 1, partErr: minio.ErrorResponse{Code: "SlowDown", StatusCode: 503}, failures: 1, wantAttempts: 2},
		{name: "exhausted_part", size: 1, partErr: minio.ErrorResponse{Code: "SlowDown", StatusCode: 503}, failures: 10, wantAttempts: 10, wantAborts: 1},
		{name: "lost_upload", size: 1, partErr: minio.ErrorResponse{Code: "NoSuchUpload", StatusCode: 404}, failures: 1, wantAttempts: 1, wantAborts: 1, abortErr: minio.ErrorResponse{Code: "NoSuchUpload", StatusCode: 404}},
		{name: "cleanup_failure", size: 1, partErr: context.DeadlineExceeded, failures: 1, wantAttempts: 1, wantAborts: 3, abortErr: minio.ErrorResponse{Code: "SlowDown", StatusCode: 503}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &minio.Client{}
			info := minio.ObjectInfo{Size: tc.size, ContentType: "application/octet-stream", UserMetadata: map[string]string{"test": "metadata"}, UserTags: map[string]string{"tag": "value"}}
			var initCalls, attempts, aborts, completes int
			var mu sync.Mutex
			init := mockey.Mock(minio.Core.NewMultipartUpload).To(func(core minio.Core, _ context.Context, bucket, object string, opts minio.PutObjectOptions) (string, error) {
				initCalls++
				assert.Same(t, client, core.Client)
				assert.Equal(t, "dst-bucket", bucket)
				assert.Equal(t, "dst-object", object)
				assert.Equal(t, map[string]string(info.UserMetadata), opts.UserMetadata)
				assert.Equal(t, map[string]string(info.UserTags), opts.UserTags)
				assert.Equal(t, info.ContentType, opts.ContentType)
				if tc.cancelAfterInit {
					cancel()
				}
				if tc.emptyID {
					return "", nil
				}
				return "own-upload", tc.initErr
			}).Build()
			defer init.UnPatch()
			part := mockey.Mock(minio.Core.CopyObjectPart).To(func(_ minio.Core, _ context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, number int, offset, length int64, headers map[string]string) (minio.CompletePart, error) {
				mu.Lock()
				defer mu.Unlock()
				attempts++
				assert.Equal(t, "own-upload", uploadID)
				assert.Empty(t, headers)
				assert.Equal(t, "src-bucket", srcBucket)
				assert.Equal(t, "src-object", srcObject)
				assert.Equal(t, "dst-bucket", dstBucket)
				assert.Equal(t, "dst-object", dstObject)
				partSize := max(int64(minioCopyPartSize), (tc.size+minioCopyMaxParts-1)/minioCopyMaxParts)
				assert.Equal(t, int64(number-1)*partSize, offset)
				assert.Equal(t, min(partSize, tc.size-offset), length)
				if attempts <= tc.failures {
					return minio.CompletePart{}, tc.partErr
				}
				return minio.CompletePart{PartNumber: number, ETag: fmt.Sprint(number)}, nil
			}).Build()
			defer part.UnPatch()
			complete := mockey.Mock(minio.Core.CompleteMultipartUpload).To(func(_ minio.Core, _ context.Context, _, _, _ string, parts []minio.CompletePart, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
				completes++
				for i, part := range parts {
					assert.Equal(t, i+1, part.PartNumber)
					assert.Equal(t, fmt.Sprint(i+1), part.ETag)
				}
				return minio.UploadInfo{ETag: "complete"}, nil
			}).Build()
			defer complete.UnPatch()
			abort := mockey.Mock(minio.Core.AbortMultipartUpload).To(func(_ minio.Core, cleanup context.Context, bucket, object, uploadID string) error {
				aborts++
				assert.NoError(t, cleanup.Err())
				deadline, ok := cleanup.Deadline()
				assert.True(t, ok)
				assert.LessOrEqual(t, time.Until(deadline), 30*time.Second)
				assert.Equal(t, "dst-bucket", bucket)
				assert.Equal(t, "dst-object", object)
				assert.Equal(t, "own-upload", uploadID)
				return tc.abortErr
			}).Build()
			defer abort.UnPatch()
			s := &MinioObjectStorage{Client: client}
			err := s.copyObjectMultipart(ctx, minio.CopyDestOptions{Bucket: "dst-bucket", Object: "dst-object"}, minio.CopySrcOptions{Bucket: "src-bucket", Object: "src-object"}, info, minioCopyPartSize)
			assert.Equal(t, tc.wantAttempts, attempts)
			assert.Equal(t, tc.wantAborts, aborts)
			switch {
			case tc.size <= 0 || tc.size > minioCopyMaxObjectSize:
				assert.ErrorIs(t, err, merr.ErrDataIntegrity)
				assert.Zero(t, initCalls)
			case tc.initErr != nil:
				assert.ErrorIs(t, err, tc.initErr)
			case tc.emptyID:
				assert.ErrorIs(t, err, merr.ErrIoFailed)
			case tc.cancelAfterInit:
				assert.ErrorIs(t, err, context.Canceled)
			case tc.wantAborts != 0:
				assert.Equal(t, tc.partErr, err, "cleanup must not replace the original error")
			default:
				require.NoError(t, err)
				assert.Equal(t, 1, completes)
			}
		})
	}
}

func TestMinioMultipartCopyWaitsBeforeAbort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	exited := make(chan struct{})
	aborted := make(chan struct{})
	init := mockey.Mock(minio.Core.NewMultipartUpload).Return("own-upload", nil).Build()
	defer init.UnPatch()
	part := mockey.Mock(minio.Core.CopyObjectPart).To(func(_ minio.Core, partCtx context.Context, _, _, _, _, _ string, _ int, _, _ int64, _ map[string]string) (minio.CompletePart, error) {
		close(started)
		<-partCtx.Done()
		<-release
		close(exited)
		// Model an in-flight call returning success after task cancellation.
		return minio.CompletePart{PartNumber: 1, ETag: "copied"}, nil
	}).Build()
	defer part.UnPatch()
	abort := mockey.Mock(minio.Core.AbortMultipartUpload).To(func(_ minio.Core, cleanup context.Context, _, _, _ string) error {
		assert.NoError(t, cleanup.Err())
		select {
		case <-exited:
		default:
			t.Error("abort raced with a live part")
		}
		close(aborted)
		return nil
	}).Build()
	defer abort.UnPatch()
	complete := mockey.Mock(minio.Core.CompleteMultipartUpload).To(func(_ minio.Core, _ context.Context, _, _, _ string, _ []minio.CompletePart, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
		t.Error("canceled upload must not complete")
		return minio.UploadInfo{}, nil
	}).Build()
	defer complete.UnPatch()
	done := make(chan error, 1)
	go func() {
		done <- (&MinioObjectStorage{}).copyObjectMultipart(ctx, minio.CopyDestOptions{}, minio.CopySrcOptions{}, minio.ObjectInfo{Size: 1}, minioCopyPartSize)
	}()
	<-started
	cancel()
	select {
	case <-aborted:
		t.Error("abort before release")
	default:
	}
	close(release)
	require.ErrorIs(t, <-done, context.Canceled)
	<-aborted
}

func TestRetryMultipartCopyError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"canceled", context.Canceled, false},
		{"deadline", context.DeadlineExceeded, false},
		{"wrapped_cancel", merr.Wrap(context.Canceled, "copy"), false},
		{"network", io.ErrUnexpectedEOF, true},
		{"empty_etag", merr.WrapErrIoFailedMsg("empty ETag"), true},
		{"permission", minio.ErrorResponse{Code: "AccessDenied", StatusCode: 403}, false},
		{"changed_source", minio.ErrorResponse{Code: "PreconditionFailed", StatusCode: 412}, false},
		{"lost_upload", minio.ErrorResponse{Code: "NoSuchUpload"}, false},
		{"slow_down", minio.ErrorResponse{Code: "SlowDown", StatusCode: 503}, true},
		{"refresh_credentials", minio.ErrorResponse{Code: "ExpiredToken", StatusCode: 403}, true},
		{"unknown_403", minio.ErrorResponse{Code: "Unknown", StatusCode: 403}, false},
		{"timeout", minio.ErrorResponse{StatusCode: 408}, true},
		{"throttle", minio.ErrorResponse{StatusCode: 429}, true},
		{"server", minio.ErrorResponse{StatusCode: 502}, true},
	} {
		t.Run(tc.name, func(t *testing.T) { assert.Equal(t, tc.retry, retryMultipartCopyError(tc.err)) })
	}
}

func TestMinioObjectStorage(t *testing.T) {
	ctx := context.Background()
	// This test deletes its bucket, so it must not share one with other test packages.
	config := objectstorage.Config{
		Address:           Params.MinioCfg.Address.GetValue(),
		AccessKeyID:       Params.MinioCfg.AccessKeyID.GetValue(),
		SecretAccessKeyID: Params.MinioCfg.SecretAccessKey.GetValue(),
		RootPath:          Params.MinioCfg.RootPath.GetValue(),

		BucketName:    fmt.Sprintf("test-minio-object-storage-%d", time.Now().UnixNano()),
		CreateBucket:  true,
		UseIAM:        false,
		CloudProvider: "minio",
	}

	t.Run("test initialize", func(t *testing.T) {
		var err error
		bucketName := config.BucketName
		config.BucketName = ""
		_, err = newMinioObjectStorageWithConfig(ctx, &config)
		assert.Error(t, err)
		config.BucketName = bucketName
		_, err = newMinioObjectStorageWithConfig(ctx, &config)
		assert.Equal(t, err, nil)
	})

	t.Run("test load", func(t *testing.T) {
		testCM, err := newMinioObjectStorageWithConfig(ctx, &config)
		assert.Equal(t, err, nil)
		defer testCM.RemoveBucket(ctx, config.BucketName)

		prepareTests := []struct {
			key   string
			value []byte
		}{
			{"abc", []byte("123")},
			{"abcd", []byte("1234")},
			{"key_1", []byte("111")},
			{"key_2", []byte("222")},
			{"key_3", []byte("333")},
		}

		for _, test := range prepareTests {
			err := testCM.PutObject(ctx, config.BucketName, test.key, bytes.NewReader(test.value), int64(len(test.value)))
			require.NoError(t, err)
		}

		loadTests := []struct {
			isvalid       bool
			loadKey       string
			expectedValue []byte

			description string
		}{
			{true, "abc", []byte("123"), "load valid key abc"},
			{true, "abcd", []byte("1234"), "load valid key abcd"},
			{true, "key_1", []byte("111"), "load valid key key_1"},
			{true, "key_2", []byte("222"), "load valid key key_2"},
			{true, "key_3", []byte("333"), "load valid key key_3"},
			{false, "key_not_exist", []byte(""), "load invalid key key_not_exist"},
			{false, "/", []byte(""), "load leading slash"},
		}

		for _, test := range loadTests {
			t.Run(test.description, func(t *testing.T) {
				if test.isvalid {
					got, err := testCM.GetObject(ctx, config.BucketName, test.loadKey, 0, 1024)
					assert.NoError(t, err)
					contentData, err := io.ReadAll(got)
					assert.NoError(t, err)
					assert.Equal(t, len(contentData), len(test.expectedValue))
					assert.Equal(t, test.expectedValue, contentData)
					statSize, err := testCM.StatObject(ctx, config.BucketName, test.loadKey)
					assert.NoError(t, err)
					assert.Equal(t, statSize, int64(len(contentData)))
					_, err = testCM.GetObject(ctx, config.BucketName, test.loadKey, 1, 1023)
					assert.NoError(t, err)
				} else {
					got, err := testCM.GetObject(ctx, config.BucketName, test.loadKey, 0, 1024)
					assert.NoError(t, err)
					_, err = io.ReadAll(got)
					errResponse := minio.ToErrorResponse(err)
					if test.loadKey == "/" {
						assert.Equal(t, errResponse.Code, "XMinioInvalidObjectName")
					} else {
						assert.Equal(t, errResponse.Code, "NoSuchKey")
					}
				}
			})
		}

		loadWithPrefixTests := []struct {
			isvalid       bool
			prefix        string
			expectedValue [][]byte

			description string
		}{
			{true, "abc", [][]byte{[]byte("123"), []byte("1234")}, "load with valid prefix abc"},
			{true, "key_", [][]byte{[]byte("111"), []byte("222"), []byte("333")}, "load with valid prefix key_"},
			{true, "prefix", [][]byte{}, "load with valid but not exist prefix prefix"},
		}

		for _, test := range loadWithPrefixTests {
			t.Run(test.description, func(t *testing.T) {
				gotk, _, err := listAllObjectsWithPrefixAtBucket(ctx, testCM, config.BucketName, test.prefix, false)
				assert.NoError(t, err)
				assert.Equal(t, len(test.expectedValue), len(gotk))
				for _, key := range gotk {
					err := testCM.RemoveObject(ctx, config.BucketName, key)
					assert.NoError(t, err)
				}
			})
		}
	})

	t.Run("test list", func(t *testing.T) {
		testCM, err := newMinioObjectStorageWithConfig(ctx, &config)
		assert.Equal(t, err, nil)
		defer testCM.RemoveBucketWithOptions(ctx, config.BucketName, minio.RemoveBucketOptions{
			ForceDelete: true,
		})

		prepareTests := []struct {
			valid bool
			key   string
			value []byte
		}{
			// {true, "abc/", []byte("123")}, will write as abc
			{true, "abc/d", []byte("1234")},
			{true, "abc/d/e", []byte("12345")},
			{true, "abc/e/d", []byte("12354")},
			{true, "key_/1/1", []byte("111")},
			{true, "key_/1/2", []byte("222")},
			{true, "key_/1/2/3", []byte("333")},
			{true, "key_/2/3", []byte("333")},
		}

		for _, test := range prepareTests {
			t.Run(test.key, func(t *testing.T) {
				err := testCM.PutObject(ctx, config.BucketName, test.key, bytes.NewReader(test.value), int64(len(test.value)))
				require.Equal(t, test.valid, err == nil, err)
			})
		}

		insertWithPrefixTests := []struct {
			recursive     bool
			prefix        string
			expectedValue []string
		}{
			{true, "abc/", []string{"abc/d", "abc/e/d"}},
			{true, "key_/", []string{"key_/1/1", "key_/1/2", "key_/2/3"}},
			{false, "abc/", []string{"abc/d", "abc/e/"}},
			{false, "key_/", []string{"key_/1/", "key_/2/"}},
		}

		for _, test := range insertWithPrefixTests {
			t.Run(fmt.Sprintf("prefix: %s, recursive: %t", test.prefix, test.recursive), func(t *testing.T) {
				gotk, _, err := listAllObjectsWithPrefixAtBucket(ctx, testCM, config.BucketName, test.prefix, test.recursive)
				assert.NoError(t, err)
				assert.Equal(t, len(test.expectedValue), len(gotk))
				for _, key := range gotk {
					assert.Contains(t, test.expectedValue, key)
				}
			})
		}
	})

	t.Run("test useIAM", func(t *testing.T) {
		// newMinioObjectStorageWithConfig validates IAM credentials by calling
		// BucketExists against the configured endpoint. With invalid IAM
		// credentials on a host where the endpoint is unreachable, the dial
		// blocks long enough that retry.Do (CheckBucketRetryAttempts=20) drags
		// the whole package out to the 10min testing.M timeout. Bound each
		// call with a short context so it fails fast regardless of
		// environment; the test only asserts an error is returned. Mirrors
		// the Azure fix in PR #49814.
		var err error
		config.UseIAM = true
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = newMinioObjectStorageWithConfig(cctx, &config)
		cancel()
		assert.Error(t, err)
		config.UseIAM = false
	})

	t.Run("test ssl", func(t *testing.T) {
		// Same endpoint-unreachable hang as the "test useIAM" subtest above:
		// UseSSL=true with a dummy CA cert against a non-TLS minio endpoint
		// keeps retry.Do dialing until the testing.M timeout. Bound it.
		var err error
		config.UseSSL = true
		config.SslCACert = "/tmp/dummy.crt"
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = newMinioObjectStorageWithConfig(cctx, &config)
		cancel()
		assert.Error(t, err)
		config.UseSSL = false
	})

	t.Run("test cloud provider", func(t *testing.T) {
		// Same endpoint-unreachable hang as the "test useIAM" subtest above.
		var err error
		cloudProvider := config.CloudProvider
		config.CloudProvider = "aliyun"
		config.UseIAM = true
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = newMinioObjectStorageWithConfig(cctx, &config)
		cancel()
		assert.Error(t, err)
		config.UseIAM = false
		cctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		_, err = newMinioObjectStorageWithConfig(cctx, &config)
		cancel()
		assert.Error(t, err)
		config.CloudProvider = "gcp"
		_, err = newMinioObjectStorageWithConfig(ctx, &config)
		assert.NoError(t, err)
		config.CloudProvider = cloudProvider
	})

	t.Run("test CopyObject", func(t *testing.T) {
		testCM, err := newMinioObjectStorageWithConfig(ctx, &config)
		assert.NoError(t, err)
		defer testCM.RemoveBucketWithOptions(ctx, config.BucketName, minio.RemoveBucketOptions{
			ForceDelete: true,
		})

		// Test successful copy
		t.Run("copy object successfully", func(t *testing.T) {
			srcKey := "copy_test/src/file1"
			dstKey := "copy_test/dst/file1"
			value := []byte("test data for copy")

			// Put source object
			err := testCM.PutObject(ctx, config.BucketName, srcKey, bytes.NewReader(value), int64(len(value)))
			require.NoError(t, err)

			// Copy object
			err = testCM.CopyObjectCrossBucket(ctx, config.BucketName, srcKey, config.BucketName, dstKey)
			assert.NoError(t, err)

			// Verify destination object exists and has correct content
			dstReader, err := testCM.GetObject(ctx, config.BucketName, dstKey, 0, 1024)
			assert.NoError(t, err)
			dstData, err := io.ReadAll(dstReader)
			assert.NoError(t, err)
			assert.Equal(t, value, dstData)

			// Verify source object still exists
			srcReader, err := testCM.GetObject(ctx, config.BucketName, srcKey, 0, 1024)
			assert.NoError(t, err)
			srcData, err := io.ReadAll(srcReader)
			assert.NoError(t, err)
			assert.Equal(t, value, srcData)

			// Clean up
			err = testCM.RemoveObject(ctx, config.BucketName, srcKey)
			assert.NoError(t, err)
			err = testCM.RemoveObject(ctx, config.BucketName, dstKey)
			assert.NoError(t, err)
		})

		// Test copy non-existent source
		t.Run("copy non-existent source object", func(t *testing.T) {
			srcKey := "copy_test/not_exist/file"
			dstKey := "copy_test/dst/file"

			err := testCM.CopyObjectCrossBucket(ctx, config.BucketName, srcKey, config.BucketName, dstKey)
			assert.Error(t, err)
		})

		// Test copy overwrite existing object
		t.Run("copy and overwrite existing object", func(t *testing.T) {
			srcKey := "copy_test/src3/file3"
			dstKey := "copy_test/dst3/file3"
			srcValue := []byte("new content")
			oldValue := []byte("old content")

			// Put destination with old content
			err := testCM.PutObject(ctx, config.BucketName, dstKey, bytes.NewReader(oldValue), int64(len(oldValue)))
			require.NoError(t, err)

			// Put source with new content
			err = testCM.PutObject(ctx, config.BucketName, srcKey, bytes.NewReader(srcValue), int64(len(srcValue)))
			require.NoError(t, err)

			// Copy (should overwrite)
			err = testCM.CopyObjectCrossBucket(ctx, config.BucketName, srcKey, config.BucketName, dstKey)
			assert.NoError(t, err)

			// Verify destination has new content
			dstReader, err := testCM.GetObject(ctx, config.BucketName, dstKey, 0, 1024)
			assert.NoError(t, err)
			dstData, err := io.ReadAll(dstReader)
			assert.NoError(t, err)
			assert.Equal(t, srcValue, dstData)

			// Clean up
			err = testCM.RemoveObject(ctx, config.BucketName, srcKey)
			assert.NoError(t, err)
			err = testCM.RemoveObject(ctx, config.BucketName, dstKey)
			assert.NoError(t, err)
		})

		// Test copy large object
		t.Run("copy large object", func(t *testing.T) {
			srcKey := "copy_test/src4/large_file"
			dstKey := "copy_test/dst4/large_file"

			// Create 5MB data
			largeData := make([]byte, 5*1024*1024)
			for i := range largeData {
				largeData[i] = byte(i % 256)
			}

			err := testCM.PutObject(ctx, config.BucketName, srcKey, bytes.NewReader(largeData), int64(len(largeData)))
			require.NoError(t, err)

			// Copy large object
			err = testCM.CopyObjectCrossBucket(ctx, config.BucketName, srcKey, config.BucketName, dstKey)
			assert.NoError(t, err)

			// Verify content
			dstReader, err := testCM.GetObject(ctx, config.BucketName, dstKey, 0, int64(len(largeData)))
			assert.NoError(t, err)
			dstData, err := io.ReadAll(dstReader)
			assert.NoError(t, err)
			assert.Equal(t, largeData, dstData)

			// Clean up
			err = testCM.RemoveObject(ctx, config.BucketName, srcKey)
			assert.NoError(t, err)
			err = testCM.RemoveObject(ctx, config.BucketName, dstKey)
			assert.NoError(t, err)
		})

		// Test copy empty object
		t.Run("copy empty object", func(t *testing.T) {
			srcKey := "copy_test/src5/empty_file"
			dstKey := "copy_test/dst5/empty_file"
			emptyData := []byte{}

			// Put empty object
			err := testCM.PutObject(ctx, config.BucketName, srcKey, bytes.NewReader(emptyData), 0)
			require.NoError(t, err)

			// Copy empty object
			err = testCM.CopyObjectCrossBucket(ctx, config.BucketName, srcKey, config.BucketName, dstKey)
			assert.NoError(t, err)

			// Verify destination exists and has size 0
			size, err := testCM.StatObject(ctx, config.BucketName, dstKey)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), size)

			// Clean up
			err = testCM.RemoveObject(ctx, config.BucketName, srcKey)
			assert.NoError(t, err)
			err = testCM.RemoveObject(ctx, config.BucketName, dstKey)
			assert.NoError(t, err)
		})

		// Test copy with nested path
		t.Run("copy object with nested path", func(t *testing.T) {
			srcKey := "copy_test/src6/file6"
			dstKey := "copy_test/dst6/nested/deep/path/file6"
			value := []byte("test data for nested path copy")

			// Put source object
			err := testCM.PutObject(ctx, config.BucketName, srcKey, bytes.NewReader(value), int64(len(value)))
			require.NoError(t, err)

			// Copy to nested path
			err = testCM.CopyObjectCrossBucket(ctx, config.BucketName, srcKey, config.BucketName, dstKey)
			assert.NoError(t, err)

			// Verify destination exists and has correct content
			dstReader, err := testCM.GetObject(ctx, config.BucketName, dstKey, 0, 1024)
			assert.NoError(t, err)
			dstData, err := io.ReadAll(dstReader)
			assert.NoError(t, err)
			assert.Equal(t, value, dstData)

			// Clean up
			err = testCM.RemoveObject(ctx, config.BucketName, srcKey)
			assert.NoError(t, err)
			err = testCM.RemoveObject(ctx, config.BucketName, dstKey)
			assert.NoError(t, err)
		})
	})
}

// listAllObjectsWithPrefixAtBucket is a helper function to list all objects with same @prefix at bucket by using `ListWithPrefix`.
func listAllObjectsWithPrefixAtBucket(ctx context.Context, objectStorage ObjectStorage, bucket string, prefix string, recursive bool) ([]string, []time.Time, error) {
	var dirs []string
	var mods []time.Time
	if err := objectStorage.WalkWithObjects(ctx, bucket, prefix, recursive, func(chunkObjectInfo *ChunkObjectInfo) bool {
		dirs = append(dirs, chunkObjectInfo.FilePath)
		mods = append(mods, chunkObjectInfo.ModifyTime)
		return true
	}); err != nil {
		return nil, nil, err
	}
	return dirs, mods, nil
}

func TestMinioObjectStorageWalkWithObjectsMapsErrors(t *testing.T) {
	for _, test := range []struct {
		name       string
		statusCode int
		code       string
		expected   error
	}{
		{name: "access denied", statusCode: http.StatusForbidden, code: minioAccessDenied, expected: merr.ErrIoPermissionDenied},
		{name: "bucket not found", statusCode: http.StatusNotFound, code: minioNoSuchBucket, expected: merr.ErrIoBucketNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(test.statusCode)
				fmt.Fprintf(w, "<Error><Code>%s</Code><Message>test error</Message></Error>", test.code)
			}))
			defer server.Close()

			client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{
				Creds:  credentials.NewStaticV4("access-key", "secret-key", ""),
				Secure: false,
			})
			require.NoError(t, err)

			storage := &MinioObjectStorage{Client: client}
			err = storage.WalkWithObjects(context.Background(), "test-bucket", "test-prefix", true, func(*ChunkObjectInfo) bool {
				return true
			})
			assert.ErrorIs(t, err, test.expected)
		})
	}

	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		client, err := minio.New("127.0.0.1:1", &minio.Options{
			Creds:  credentials.NewStaticV4("access-key", "secret-key", ""),
			Secure: false,
		})
		require.NoError(t, err)

		storage := &MinioObjectStorage{Client: client}
		err = storage.WalkWithObjects(ctx, "test-bucket", "test-prefix", true, func(*ChunkObjectInfo) bool {
			return true
		})
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestMapObjectStorageError_MinIO_NewErrors(t *testing.T) {
	tests := []struct {
		name          string
		inputError    error
		expectedError error
	}{
		// Permanent errors
		{
			name:          "AccessDenied",
			inputError:    minio.ErrorResponse{Code: "AccessDenied"},
			expectedError: merr.ErrIoPermissionDenied,
		},
		{
			name:          "InvalidAccessKeyId",
			inputError:    minio.ErrorResponse{Code: "InvalidAccessKeyId"},
			expectedError: merr.ErrIoPermissionDenied,
		},
		{
			name:          "SignatureDoesNotMatch",
			inputError:    minio.ErrorResponse{Code: "SignatureDoesNotMatch"},
			expectedError: merr.ErrIoPermissionDenied,
		},
		{
			name:          "NoSuchBucket",
			inputError:    minio.ErrorResponse{Code: "NoSuchBucket"},
			expectedError: merr.ErrIoBucketNotFound,
		},
		{
			name:          "InvalidToken",
			inputError:    minio.ErrorResponse{Code: "InvalidToken"},
			expectedError: merr.ErrIoInvalidCredentials,
		},
		{
			name:          "ExpiredToken",
			inputError:    minio.ErrorResponse{Code: "ExpiredToken"},
			expectedError: merr.ErrIoInvalidCredentials,
		},
		// Client validation errors
		{
			name:          "InvalidArgument",
			inputError:    minio.ErrorResponse{Code: "InvalidArgument"},
			expectedError: merr.ErrIoInvalidArgument,
		},
		{
			name:          "InvalidRequest",
			inputError:    minio.ErrorResponse{Code: "InvalidRequest"},
			expectedError: merr.ErrIoInvalidArgument,
		},
		{
			name:          "InvalidRange",
			inputError:    minio.ErrorResponse{Code: "InvalidRange"},
			expectedError: merr.ErrIoInvalidRange,
		},
		{
			name:          "EntityTooLarge",
			inputError:    minio.ErrorResponse{Code: "EntityTooLarge"},
			expectedError: merr.ErrIoEntityTooLarge,
		},
		{
			name:          "MaxMessageLengthExceeded",
			inputError:    minio.ErrorResponse{Code: "MaxMessageLengthExceeded"},
			expectedError: merr.ErrIoEntityTooLarge,
		},
		// Aliyun OSS specific error codes
		{
			name:          "AliyunSecurityTokenExpired",
			inputError:    minio.ErrorResponse{Code: "SecurityTokenExpired"},
			expectedError: merr.ErrIoInvalidCredentials,
		},
		{
			name:          "AliyunInvalidAccessKeyIdInactive",
			inputError:    minio.ErrorResponse{Code: "InvalidAccessKeyId.Inactive"},
			expectedError: merr.ErrIoPermissionDenied,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapObjectStorageError("test/path", tt.inputError)
			assert.True(t, errors.Is(result, tt.expectedError),
				"expected %v, got %v", tt.expectedError, result)
		})
	}
}

// A store that answers DeleteObject with HTTP 200 has deleted the object; minio-go
// still reports the response as an error. Only that case is treated as success:
// a 404 keeps surfacing as ErrIoKeyNotFound for the callers that handle it.
func TestIsDeleteSucceededWithOK(t *testing.T) {
	assert.False(t, isDeleteSucceededWithOK(nil))
	assert.True(t, isDeleteSucceededWithOK(minio.ErrorResponse{StatusCode: http.StatusOK}))

	notFound := minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: minioNoSuchKey, Message: "The specified key does not exist."}
	assert.False(t, isDeleteSucceededWithOK(notFound))
	assert.True(t, errors.Is(mapObjectStorageError("k", notFound), merr.ErrIoKeyNotFound))

	assert.False(t, isDeleteSucceededWithOK(errors.New("dial tcp: connection refused")))
}

func TestMinioObjectStorageMultipartConfiguration(t *testing.T) {
	params := paramtable.Get()
	threshold, part := &params.MinioCfg.MultipartCopyThreshold, &params.MinioCfg.MultipartCopyPartSize
	oldThreshold, oldPart := threshold.GetValue(), part.GetValue()
	require.NoError(t, params.Save(threshold.Key, threshold.DefaultValue))
	t.Cleanup(func() { params.Save(threshold.Key, oldThreshold); params.Save(part.Key, oldPart) })
	var mu sync.Mutex
	var size int64
	var nextPart string
	var ranges map[int]string
	var single, completed bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.Header().Set("ETag", `"source"`)
			w.Header().Set("Last-Modified", "Wed, 23 Sep 2026 00:00:00 GMT")
			if nextPart != "" {
				assert.NoError(t, params.Save(part.Key, nextPart))
			}
		case r.Method == http.MethodPost && q.Has("uploads"):
			fmt.Fprint(w, `<InitiateMultipartUploadResult><UploadId>upload</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == http.MethodPut && q.Get("uploadId") == "upload":
			n, err := strconv.Atoi(q.Get("partNumber"))
			assert.NoError(t, err)
			ranges[n] = r.Header.Get("X-Amz-Copy-Source-Range")
			fmt.Fprintf(w, `<CopyPartResult><ETag>"part-%d"</ETag></CopyPartResult>`, n)
		case r.Method == http.MethodPost && q.Get("uploadId") == "upload":
			completed = true
			fmt.Fprint(w, `<CompleteMultipartUploadResult><Bucket>target-bucket</Bucket><Key>target</Key><ETag>"complete"</ETag></CompleteMultipartUploadResult>`)
		case r.Method == http.MethodPut:
			single = true
			fmt.Fprint(w, `<CopyObjectResult><ETag>"whole"</ETag></CopyObjectResult>`)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{
		Creds: credentials.NewStaticV4("access", "secret", ""), Region: "us-east-1", BucketLookup: minio.BucketLookupPath,
	})
	require.NoError(t, err)
	store := &MinioObjectStorage{Client: client, cloudProvider: objectstorage.CloudProviderAWS}
	for _, tc := range []struct {
		name, configured, next string
		size, wantPart         int64
		multipart              bool
	}{
		{"below restored threshold", "10485760", "", 500 * 1024 * 1024, 0, false},
		{"at restored threshold", "10485760", "", 1024 * 1024 * 1024, 10 * 1024 * 1024, true},
		{"freeze part size", "134217728", "33554432", 1024*1024*1024 + 3, 128 * 1024 * 1024, true},
		{"next copy sees update", "", "", 1024*1024*1024 + 3, 32 * 1024 * 1024, true},
		{"invalid size falls back", "1", "", 1024*1024*1024 + 3, 10 * 1024 * 1024, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.configured != "" {
				require.NoError(t, params.Save(part.Key, tc.configured))
			}
			mu.Lock()
			size, nextPart = tc.size, tc.next
			ranges = make(map[int]string)
			single, completed = false, false
			mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			copied, err := store.CopyObjectCrossBucketAndGetSize(ctx, "source-bucket", "source", "target-bucket", "target")
			require.NoError(t, err)
			require.Equal(t, tc.size, copied)
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, !tc.multipart, single)
			require.Equal(t, tc.multipart, completed)
			if !tc.multipart {
				require.Empty(t, ranges)
				return
			}
			require.Len(t, ranges, int((tc.size+tc.wantPart-1)/tc.wantPart))
			for n := 1; n <= len(ranges); n++ {
				start := int64(n-1) * tc.wantPart
				require.Equal(t, fmt.Sprintf("bytes=%d-%d", start, min(start+tc.wantPart, tc.size)-1), ranges[n])
			}
		})
	}
}
