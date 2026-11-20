//go:build test

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/sourceauth"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const sourceConsumerRoot = "https://account123.blob.core.windows.net/data/table"

var sourceConsumerRegistry sourceauth.Registry
var sourceConsumerInstall sync.Once
var sourceConsumerInstallErr error

type sourceConsumerSession struct {
	status          sourceauth.CredentialStatus
	fetches, closes atomic.Int32
}

func (*sourceConsumerSession) AccountName() string       { return "account123" }
func (*sourceConsumerSession) AllowedRootURIs() []string { return []string{sourceConsumerRoot} }
func (s *sourceConsumerSession) Fetch(context.Context, sourceauth.FetchRequest) sourceauth.Credential {
	s.fetches.Add(1)
	return sourceauth.Credential{Status: s.status}
}
func (s *sourceConsumerSession) Close() { s.closes.Add(1) }

func newSourceConsumer(t *testing.T, source, format string, status sourceauth.CredentialStatus) (ExternalSpecContext, *sourceConsumerSession) {
	t.Helper()
	paramtable.Init()
	sourceConsumerInstall.Do(func() {
		sourceConsumerInstallErr = sourceauth.Install(&sourceConsumerRegistry, func(ctx context.Context, _ any, _ string) (context.Context, error) { return ctx, nil })
	})
	require.NoError(t, sourceConsumerInstallErr)
	session := &sourceConsumerSession{status: status}
	provider := "native-consumer-" + uuid.NewString()
	require.NoError(t, sourceConsumerRegistry.Register(provider, func(context.Context, sourceauth.Binding) (sourceauth.Session, error) {
		return session, nil
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	authorizer := sourceauth.WithAuthorizer(ctx, func(context.Context, sourceauth.Operation) (sourceauth.Reference, error) {
		return sourceauth.Reference{Provider: provider, Value: "test-owner/table"}, nil
	})
	spec, err := sourceauth.Prepare(authorizer, sourceauth.Operation{
		Source: source,
		Spec:   fmt.Sprintf(`{"format":%q,"extfs":{"cloud_provider":"azure","access_key_id":"account123"}}`, format),
	}, true)
	require.NoError(t, err)
	return ExternalSpecContext{Context: ctx, CollectionID: 42, Source: source, Spec: spec}, session
}

// These call the actual engine/native entry points. The callback never provides
// SAS, so every case must fail locally before an Azure request can be sent.
func TestSourceAuthorizationNativeConsumersPreserveOutcome(t *testing.T) {
	for _, status := range []sourceauth.CredentialStatus{
		sourceauth.CredentialDenied, sourceauth.CredentialExpired,
		sourceauth.CredentialTransient, sourceauth.CredentialCancelled,
		sourceauth.CredentialInvalidResponse,
	} {
		for _, consumer := range []string{"lance-explore", "iceberg-explore", "file-info", "filesystem", "packed-reader"} {
			t.Run(fmt.Sprintf("%s/%d", consumer, status), func(t *testing.T) {
				format, source := "iceberg-table", sourceConsumerRoot+"/metadata/v1.metadata.json"
				if consumer == "lance-explore" {
					format, source = "lance-table", "abfss://data@account123.dfs.core.windows.net/table"
				}
				ext, session := newSourceConsumer(t, source, format, status)
				config := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
				file := sourceConsumerRoot + "/data/part.parquet"
				var err error
				switch consumer {
				case "lance-explore", "iceberg-explore":
					_, _, err = ExploreFilesReturnManifestPath([]string{"id"}, format, t.TempDir(), source, config, ext)
				case "file-info":
					_, err = GetFileInfo("parquet", file, config, ext)
				case "filesystem":
					_, err = ReadFileWithExternalSpec(config, file, ext)
				case "packed-reader":
					schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
					var reader *FFIPackedReader
					reader, err = NewFFIPackedReaderWithFragments([]string{"id"}, "parquet",
						[]Fragment{{FilePath: file, StartRow: 0, EndRow: 1, RowCount: 1}},
						schema, []string{"id"}, 1024, config, nil, ext)
					if reader != nil {
						if err == nil {
							_, err = reader.ReadNext()
						}
						require.NoError(t, reader.Close())
					}
				}
				require.ErrorIs(t, err, sourceauth.StatusError(status))
				// The worker RPC projection must keep permanent outcomes out of
				// retries even after the native wrappers have handled the error.
				roundTrip := merr.Error(merr.Status(err))
				require.Equal(t, status, sourceauth.StatusFromError(roundTrip))
				require.Equal(t, status != sourceauth.CredentialTransient, merr.IsPermanentSourceAuthErr(roundTrip))
				require.Positive(t, session.fetches.Load(), "must exercise the actual credential callback")
				require.EqualValues(t, 1, session.closes.Load(), "native consumer must release its provider")
			})
		}
	}
}

func TestSourceAuthorizationNativeConsumersRejectOtherAuthority(t *testing.T) {
	for _, file := range []string{
		"https://otheraccount.blob.core.windows.net/data/table/file.parquet",
		"https://account123.blob.core.usgovcloudapi.net/data/table/file.parquet",
		"https://account123.blob.core.windows.net/other/table/file.parquet",
		"https://account123.blob.core.windows.net/data/table-other/file.parquet",
		sourceConsumerRoot + "/%2fescape",
	} {
		t.Run(file, func(t *testing.T) {
			ext, session := newSourceConsumer(t, sourceConsumerRoot, "lance-table", sourceauth.CredentialInvalidResponse)
			config := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
			_, err := ReadFileWithExternalSpec(config, file, ext)
			require.ErrorIs(t, err, merr.ErrSourceAuthDenied)
			require.Zero(t, session.fetches.Load(), "must reject before requesting credentials")
			require.EqualValues(t, 1, session.closes.Load())
		})
	}
}
