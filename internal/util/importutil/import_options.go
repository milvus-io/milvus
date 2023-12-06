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
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// Extra option keys to pass through import API within []*commonpb.KeyValuePair
const (
	Bucket            = "bucket"   // the source files' minio bucket
	StartTs           = "start_ts" // start timestamp to filter data, only data between StartTs and EndTs will be imported
	EndTs             = "end_ts"   // end timestamp to filter data, only data between StartTs and EndTs will be imported
	BackupFlag        = "backup"
	StorageType       = "storageType"
	Address           = "address"
	AccessKeyID       = "accessKeyId"
	SecretAccessKeyID = "secretAccessKey"
	UseSSL            = "useSSL"
	RootPath          = "rootPath"
	UseIAM            = "useIAM"
	CloudProvider     = "cloudProvider"
	IamEndpoint       = "iamEndpoint"
	UseVirtualHost    = "useVirtualHost"
	Region            = "region"
	RequestTimeoutMs  = "requestTimeoutMS"
	OptionFormat      = "start_ts: 10-digit physical timestamp, e.g. 1665995420, default 0 \n" +
		"end_ts: 10-digit physical timestamp, e.g. 1665995420, default math.MaxInt \n"
)

type ImportOptions struct {
	OnlyValidate      bool
	TsStartPoint      uint64
	TsEndPoint        uint64
	IsBackup          bool // whether is triggered by backup tool
	StorageType       string
	Address           string
	BucketName        string
	AccessKeyID       string
	SecretAccessKeyID string
	UseSSL            bool
	RootPath          string
	UseIAM            bool
	CloudProvider     string
	IamEndpoint       string
	UseVirtualHost    bool
	Region            string
	RequestTimeoutMs  int64
}

func DefaultImportOptions() *ImportOptions {
	options := &ImportOptions{
		StorageType:       "",
		Address:           "",
		BucketName:        "",
		AccessKeyID:       "",
		SecretAccessKeyID: "",
		UseSSL:            false,
		RootPath:          "",
		UseIAM:            false,
		CloudProvider:     "",
		OnlyValidate:      false,
		TsStartPoint:      0,
		TsEndPoint:        math.MaxUint64,
	}
	return options
}

func (o *ImportOptions) String() string {
	return fmt.Sprintf("OnlyValidate: %v, "+
		"TsStartPoint: %d, "+
		"TsEndPoint: %d, "+
		"IsBackup: %v, "+
		"StorageType: %s, "+
		"Address: %s, "+
		"BucketName: %s, "+
		"AccessKeyID: %s, "+
		"SecretAccessKey: %s, "+
		"UseSSL: %v, "+
		"RootPath: %s, "+
		"UseIAM: %v, "+
		"CloudProvider: %s, "+
		"IamEndpoint: %s, "+
		"UseVirtualHost: %v, "+
		"Region: %s, "+
		"RequestTimeoutMs: %d",
		o.OnlyValidate, o.TsStartPoint, o.TsEndPoint, o.IsBackup, o.StorageType, o.Address, o.BucketName, o.AccessKeyID, o.SecretAccessKeyID, o.UseSSL, o.RootPath, o.UseIAM, o.CloudProvider, o.IamEndpoint, o.UseVirtualHost, o.Region, o.RequestTimeoutMs)
}

// ValidateOptions the options is illegal, return nil if illegal, return error if not.
// Illegal options:
//
//	start_ts: 10-digit physical timestamp, e.g. 1665995420
//	end_ts: 10-digit physical timestamp, e.g. 1665995420
func ValidateOptions(options []*commonpb.KeyValuePair) error {
	optionMap := funcutil.KeyValuePair2Map(options)
	// StartTs should be int
	_, ok := optionMap[StartTs]
	var startTs uint64
	var endTs uint64 = math.MaxInt64
	var err error
	if ok {
		startTs, err = strconv.ParseUint(optionMap[StartTs], 10, 64)
		if err != nil {
			return err
		}
	}
	// EndTs should be int
	_, ok = optionMap[EndTs]
	if ok {
		endTs, err = strconv.ParseUint(optionMap[EndTs], 10, 64)
		if err != nil {
			return err
		}
	}
	if startTs > endTs {
		return merr.WrapErrImportFailed("start_ts shouldn't be larger than end_ts")
	}
	return nil
}

func ParseImportOptions(kvs []*commonpb.KeyValuePair) (*ImportOptions, error) {
	options := DefaultImportOptions()
	for _, kv := range kvs {
		value := kv.Value
		switch kv.Key {
		case StartTs:
			uint64Value, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %q to uint64 from import parameters: %v", value, err)
			}
			options.TsStartPoint = uint64Value
		case EndTs:
			uint64Value, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %q to uint64 from import parameters: %v", value, err)
			}
			options.TsEndPoint = uint64Value
		case BackupFlag:
			boolValue, err := strconv.ParseBool(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %q to bool from import parameters: %v", value, err)
			}
			options.IsBackup = boolValue
		case StorageType:
			options.StorageType = value
		case Address:
			options.Address = value
		case Bucket:
			options.BucketName = value
		case AccessKeyID:
			options.AccessKeyID = value
		case SecretAccessKeyID:
			options.SecretAccessKeyID = value
		case UseSSL:
			boolValue, err := strconv.ParseBool(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %q to bool: %v from import parameters", value, err)
			}
			options.UseSSL = boolValue
		case RootPath:
			options.RootPath = value
		case UseIAM:
			boolValue, err := strconv.ParseBool(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %q to bool from import parameters: %v", value, err)
			}
			options.UseIAM = boolValue
		case CloudProvider:
			options.CloudProvider = value
		case IamEndpoint:
			options.IamEndpoint = value
		case UseVirtualHost:
			boolValue, err := strconv.ParseBool(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %q to bool from import parameters: %v", value, err)
			}
			options.UseVirtualHost = boolValue
		case Region:
			options.Region = value
		case RequestTimeoutMs:
			intValue, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %q to int64 from import parameters: %v", value, err)
			}
			options.RequestTimeoutMs = intValue
		default:
			return nil, fmt.Errorf("unknown key %q from import parameters", kv.Key)
		}
	}
	return options, nil
}

// IsBackup returns if the request is triggered by backup tool
func IsBackup(options []*commonpb.KeyValuePair) bool {
	isBackup, err := funcutil.GetAttrByKeyFromRepeatedKV(BackupFlag, options)
	if err != nil || strings.ToLower(isBackup) != "true" {
		return false
	}
	return true
}
