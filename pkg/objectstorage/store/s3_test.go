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

package store

import (
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
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestS3StoreCopy(t *testing.T) {
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
		{name: "negative size", size: -1},
		{name: "tiny_threshold", size: minioCopyPartSize - 1, threshold: "1"},
		{name: "minimum_multipart", size: minioCopyPartSize, threshold: "1", wantMultipart: true},
		{name: "old_default", size: 500 * 1024 * 1024},
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
				threshold = strconv.FormatInt(DefaultS3Options().MultipartThreshold, 10)
			}
			thresholdBytes, err := strconv.ParseInt(threshold, 10, 64)
			require.NoError(t, err)
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
			mockMultipart := mockey.Mock((*S3Store).copyObjectMultipart).To(
				func(_ *S3Store, _ context.Context, dst minio.CopyDestOptions, src minio.CopySrcOptions, info minio.ObjectInfo) error {
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
			client, err := objectstorage.NewMinioClient(context.Background(), &config)
			options := DefaultS3Options()
			options.MultipartThreshold = thresholdBytes
			options.DisableMultipart = objectstorage.ResolveCloudProvider(&config) == objectstorage.CloudProviderGCP
			objectStorage := &S3Store{client: client, options: options}
			require.NoError(t, err)
			assert.Equal(t, originalConfig, config, "client construction must not change shared configuration")
			result, err := objectStorage.Copy(context.Background(), CopyRequest{Source: Object{Bucket: "src-bucket", Key: "src-object"}, Destination: Object{Bucket: dstBucket, Key: "dst-object"}})
			if tc.size < 0 {
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
				assert.Zero(t, copyCalls+multipartCalls)
				return
			}
			if tc.statErr != nil {
				require.ErrorIs(t, err, tc.statErr)
				assert.Zero(t, copyCalls+multipartCalls)
				return
			}
			if tc.copyErr != nil {
				require.ErrorIs(t, err, tc.copyErr)
				require.Equal(t, int64(0), result)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.size, result)
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
func TestS3StoreSingleCopyResponse(t *testing.T) {
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
			result, err := (&S3Store{client: client, options: DefaultS3Options()}).Copy(ctx, CopyRequest{Source: Object{Bucket: "src-bucket", Key: "source"}, Destination: Object{Bucket: "dst-bucket", Key: "target"}})
			require.EqualValues(t, 1, puts.Load(), "must reach the real copy response")
			if tc.wantError {
				require.Error(t, err)
				require.Equal(t, int64(0), result, "unconfirmed copies must not count bytes")
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.size, result)
			}
		})
	}
}

// Exercise real SDK requests, including its HTTP 200 error decoding, without
// allocating or transferring the source object's bytes.
func TestS3StoreMultipartCopyRequests(t *testing.T) {
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
			options := DefaultS3Options()
			options.MultipartThreshold = 500 * 1024 * 1024
			objectStorage := &S3Store{client: client, options: options}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			result, err := objectStorage.Copy(ctx, CopyRequest{Source: Object{Bucket: "src-bucket", Key: "src-object"}, Destination: Object{Bucket: "dst-bucket", Key: "dst-object"}})
			// Wait for HTTP handlers as well as client calls before inspecting counters.
			server.Close()
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, 1, heads)
			assert.Equal(t, minioCopyPartConcurrency, peak)
			if scenario == "part_access_denied" || scenario == "complete_error" || scenario == "empty_complete_etag" {
				require.Error(t, err)
				if scenario != "empty_complete_etag" {
					assert.Equal(t, "AccessDenied", minio.ToErrorResponse(err).Code)
				}
				assert.Equal(t, int64(0), result)
				assert.True(t, aborted)
				if scenario == "part_access_denied" {
					assert.False(t, finished)
					assert.Equal(t, 1, attempts[2])
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, size, result)
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
			s := &S3Store{client: client, options: DefaultS3Options()}
			err := s.copyObjectMultipart(ctx, minio.CopyDestOptions{Bucket: "dst-bucket", Object: "dst-object"}, minio.CopySrcOptions{Bucket: "src-bucket", Object: "src-object"}, info)
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
		done <- (&S3Store{options: DefaultS3Options()}).copyObjectMultipart(ctx, minio.CopyDestOptions{}, minio.CopySrcOptions{}, minio.ObjectInfo{Size: 1})
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

func TestRunBounded(t *testing.T) {
	permits := make(chan struct{}, 2)
	operation := func(context.Context, int) error { return nil }
	require.Error(t, RunBounded(context.Background(), []int{1}, nil, operation))
	require.Error(t, RunBounded[int](context.Background(), nil, permits, nil))
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var completed atomic.Int64
	done := make(chan error, 1)
	go func() {
		done <- RunBounded(ctx, make([]int, 8), permits, func(context.Context, int) error {
			started <- struct{}{}
			<-release
			completed.Add(1)
			return nil
		})
	}()
	<-started
	<-started
	cancel()
	select {
	case <-done:
		t.Fatal("returned before active operations exited")
	default:
	}
	close(release)
	require.ErrorIs(t, <-done, context.Canceled)
	require.EqualValues(t, 2, completed.Load())
	require.Empty(t, permits)
	require.NoError(t, RunBounded(context.Background(), []int{1, 2, 3}, permits, func(context.Context, int) error { completed.Add(1); return nil }))
	require.EqualValues(t, 5, completed.Load())
	// Exercise cancellation with an available permit: either select branch
	// must prevent callbacks and return the permit.
	for i := 0; i < 32; i++ {
		require.ErrorIs(t, RunBounded(ctx, []int{1}, permits, func(context.Context, int) error { t.Fatal("ran after cancellation"); return nil }), context.Canceled)
		require.Empty(t, permits)
	}
	require.ErrorIs(t, RunBounded(context.Background(), []int{1}, permits, func(context.Context, int) error { return context.DeadlineExceeded }), context.DeadlineExceeded)
	require.Empty(t, permits)
}

func TestS3StoreOptions(t *testing.T) {
	for _, change := range []func(*S3Options){func(o *S3Options) { o.PartConcurrency = 0 }, func(o *S3Options) { o.PartSize = 0 }, func(o *S3Options) { o.PartRetryAttempts = 0 }, func(o *S3Options) { o.MultipartThreshold = -1 }, func(o *S3Options) { o.PartSize = 1 << 40 }} {
		o := DefaultS3Options()
		change(&o)
		_, err := NewS3Store(&minio.Client{}, o)
		require.Error(t, err)
	}
	_, err := NewS3Store(nil, DefaultS3Options())
	require.Error(t, err)
	_, err = NewS3Store(&minio.Client{}, DefaultS3Options())
	require.NoError(t, err)
}

func TestS3StoreReadWrite(t *testing.T) {
	payload := []byte("snapshot metadata")
	var received []byte
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bucket/missing" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)
			return
		}
		if r.URL.Path == "/bucket/denied" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>denied</Message></Error>`)
			return
		}
		require.Equal(t, "/bucket/root/metadata.json", r.URL.Path)
		switch r.Method {
		case http.MethodPut:
			var err error
			received, err = io.ReadAll(r.Body)
			require.NoError(t, err)
			w.Header().Set("ETag", `"etag"`)
		case http.MethodGet:
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("ETag", `"etag"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			_, _ = w.Write(payload)
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	client, err := minio.New(strings.TrimPrefix(server.URL, "https://"), &minio.Options{Region: "us-east-1", Secure: true, Transport: server.Client().Transport, Creds: credentials.NewStaticV4("test-access", "test-secret", "")})
	require.NoError(t, err)
	store, err := NewS3Store(client, DefaultS3Options())
	require.NoError(t, err)
	ctx := context.Background()
	object := Object{Bucket: "bucket", Key: "root/metadata.json"}
	require.NoError(t, store.Write(ctx, object, payload))
	require.Equal(t, payload, received)
	got, err := store.Read(ctx, object)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	for _, key := range []string{"missing", "denied"} {
		_, err := store.Read(ctx, Object{Bucket: "bucket", Key: key})
		require.Error(t, err)
		require.Equal(t, map[string]string{"missing": "NoSuchKey", "denied": "AccessDenied"}[key], minio.ToErrorResponse(err).Code)
	}
	require.Error(t, store.Write(ctx, Object{Bucket: "bucket", Key: "denied"}, payload))
	_, err = store.Read(ctx, Object{Bucket: "!", Key: "invalid"})
	require.Error(t, err)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = store.Read(canceled, object)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, store.Write(canceled, object, payload), context.Canceled)
}
