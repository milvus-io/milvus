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

package importutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func Test_ValidateOptions(t *testing.T) {
	assert.NoError(t, ValidateOptions([]*commonpb.KeyValuePair{}))
	assert.NoError(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "1666007457"},
		{Key: "end_ts", Value: "1666007459"},
	}))
	assert.NoError(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "0"},
		{Key: "end_ts", Value: "0"},
	}))
	assert.NoError(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "0"},
		{Key: "end_ts", Value: "1666007457"},
	}))
	assert.Error(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "-1"},
		{Key: "end_ts", Value: "-1"},
	}))
	assert.Error(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "2"},
		{Key: "end_ts", Value: "1"},
	}))
	assert.Error(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "3.14"},
		{Key: "end_ts", Value: "1666007457"},
	}))
	assert.Error(t, ValidateOptions([]*commonpb.KeyValuePair{
		{Key: "start_ts", Value: "1666007457"},
		{Key: "end_ts", Value: "3.14"},
	}))
}

func TestParseImportOptions(t *testing.T) {
	kvs := []*commonpb.KeyValuePair{
		{Key: StartTs, Value: "12345"},
		{Key: EndTs, Value: "67890"},
		{Key: BackupFlag, Value: "false"},
		{Key: StorageType, Value: "s3"},
		{Key: Address, Value: "example.com"},
		{Key: Bucket, Value: "my-bucket"},
		{Key: AccessKeyID, Value: "my-access-key"},
		{Key: SecretAccessKeyID, Value: "my-secret-key"},
		{Key: UseSSL, Value: "true"},
		{Key: RootPath, Value: "/path/to/files"},
		{Key: UseIAM, Value: "false"},
		{Key: CloudProvider, Value: "aws"},
		{Key: IamEndpoint, Value: "iam.example.com"},
		{Key: UseVirtualHost, Value: "true"},
		{Key: Region, Value: "us-west-1"},
		{Key: RequestTimeoutMs, Value: "5000"},
	}

	expectedOptions := &ImportOptions{
		TsStartPoint:      12345,
		TsEndPoint:        67890,
		IsBackup:          false,
		StorageType:       "s3",
		Address:           "example.com",
		BucketName:        "my-bucket",
		AccessKeyID:       "my-access-key",
		SecretAccessKeyID: "my-secret-key",
		UseSSL:            true,
		RootPath:          "/path/to/files",
		UseIAM:            false,
		CloudProvider:     "aws",
		IamEndpoint:       "iam.example.com",
		UseVirtualHost:    true,
		Region:            "us-west-1",
		RequestTimeoutMs:  5000,
	}

	// No error
	options, err := ParseImportOptions(kvs)
	assert.NoError(t, err)
	assert.Equal(t, expectedOptions, options)

	// Error for unknown key
	unknownKeyKvs := []*commonpb.KeyValuePair{
		{Key: "unknown_key", Value: "value"},
	}
	_, err = ParseImportOptions(unknownKeyKvs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown key")

	// Error for invalid value type - bool
	invalidBoolValueKvs := []*commonpb.KeyValuePair{
		{Key: BackupFlag, Value: "invalid"},
	}
	_, err = ParseImportOptions(invalidBoolValueKvs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to convert")

	invalidBoolValueKvs2 := []*commonpb.KeyValuePair{
		{Key: UseSSL, Value: "invalid"},
	}
	_, err = ParseImportOptions(invalidBoolValueKvs2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to convert")

	// Error for invalid value type - uint64
	invalidUint64ValueKvs := []*commonpb.KeyValuePair{
		{Key: StartTs, Value: "invalid"},
	}
	_, err = ParseImportOptions(invalidUint64ValueKvs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to convert")

	invalidUint64ValueKvs2 := []*commonpb.KeyValuePair{
		{Key: EndTs, Value: "invalid"},
	}
	_, err = ParseImportOptions(invalidUint64ValueKvs2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to convert")

	// Error for invalid value type - int64
	invalidInt64ValueKvs := []*commonpb.KeyValuePair{
		{Key: RequestTimeoutMs, Value: "invalid"},
	}
	_, err = ParseImportOptions(invalidInt64ValueKvs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to convert")

	// Empty input
	emptyKvs := []*commonpb.KeyValuePair{}
	_, err = ParseImportOptions(emptyKvs)
	assert.NoError(t, err)

	// Missing keys
	missingKeysKvs := []*commonpb.KeyValuePair{
		{Key: StartTs, Value: "12345"},
		{Key: EndTs, Value: "67890"},
	}
	_, err = ParseImportOptions(missingKeysKvs)
	assert.NoError(t, err)
}

func Test_IsBackup(t *testing.T) {
	isBackup := IsBackup([]*commonpb.KeyValuePair{
		{Key: "backup", Value: "true"},
	})
	assert.Equal(t, true, isBackup)
	isBackup2 := IsBackup([]*commonpb.KeyValuePair{
		{Key: "backup", Value: "True"},
	})
	assert.Equal(t, true, isBackup2)
	falseBackup := IsBackup([]*commonpb.KeyValuePair{
		{Key: "backup", Value: "false"},
	})
	assert.Equal(t, false, falseBackup)
	noBackup := IsBackup([]*commonpb.KeyValuePair{
		{Key: "backup", Value: "false"},
	})
	assert.Equal(t, false, noBackup)
}
