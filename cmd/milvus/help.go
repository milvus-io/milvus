package milvus

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	RunCmd = "run"
)

var (
	usageLine = fmt.Sprintf("Usage:\n"+
		"%s\n%s\n%s\n%s\n", runLine, stopLine, mckLine, serverTypeLine)

	serverTypeLine = `
[server type]
	` + strings.Join(typeutil.ServerTypeList(), "\n\t") + `
	mixture
`
	runLine = `
milvus run [server type] [flags]
	Start a Milvus Server.
	Tips: Only the server type is 'mixture', flags about starting server can be used.
[flags]
	-rootcoord 'true'
		Start the rootcoord server.
	-querycoord 'true'
		Start the querycoord server.
	-datacoord 'true'
		Start the datacoord server.
	-alias ''
		Set alias
`
	stopLine = `
milvus stop [server type] [flags]
	Stop a Milvus Server.
[flags]
	-alias ''
		Set alias
`
	mckLine = `
milvus mck run [flags]
	Milvus data consistency check.
	Tips: The flags are optional.
[flags]
	-etcdIp ''
		Ip to connect the ectd server.
	-etcdRootPath ''
		The root path of operating the etcd data.
	-minioAddress ''
		Address to connect the minio server.
	-minioUsername ''
		The username to login the minio server.
	-minioPassword ''
		The password to login the minio server.
	-minioUseSSL 'false'
		Whether to use the ssl to connect the minio server.
	-minioBucketName ''
		The bucket to operate the data in it

milvus mck cleanTrash [flags]
	Clean the back inconsistent data
	Tips: The flags is the same as its of the 'milvus mck [flags]'
`
)
