package typeutil

import (
	"fmt"
	"path"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func GetStorageURI(protocol, pathPrefix string, segmentID int64) (string, error) {
	switch protocol {
	case "s3":
		var scheme string
		if paramtable.Get().MinioCfg.UseSSL.GetAsBool() {
			scheme = "https"
		} else {
			scheme = "http"
		}
		if pathPrefix != "" {
			cleanPath := path.Clean(pathPrefix)
			return fmt.Sprintf("s3://%s:%s@%s/%s/%d?scheme=%s&endpoint_override=%s&allow_bucket_creation=true", paramtable.Get().MinioCfg.AccessKeyID.GetValue(), paramtable.Get().MinioCfg.SecretAccessKey.GetValue(), paramtable.Get().MinioCfg.BucketName.GetValue(), cleanPath, segmentID, scheme, paramtable.Get().MinioCfg.Address.GetValue()), nil
		} else {
			return fmt.Sprintf("s3://%s:%s@%s/%d?scheme=%s&endpoint_override=%s&allow_bucket_creation=true", paramtable.Get().MinioCfg.AccessKeyID.GetValue(), paramtable.Get().MinioCfg.SecretAccessKey.GetValue(), paramtable.Get().MinioCfg.BucketName.GetValue(), segmentID, scheme, paramtable.Get().MinioCfg.Address.GetValue()), nil
		}
	case "file":
		if pathPrefix != "" {
			cleanPath := path.Clean(pathPrefix)
			return fmt.Sprintf("file://%s/%d", cleanPath, segmentID), nil
		} else {
			return fmt.Sprintf("file://%d", segmentID), nil
		}
	default:
		return "", merr.WrapErrParameterInvalidMsg("unsupported schema %s", protocol)
	}
}
