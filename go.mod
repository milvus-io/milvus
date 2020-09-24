module github.com/czs007/suvlim

go 1.15

require (
	cloud.google.com/go/bigquery v1.4.0 // indirect
	code.cloudfoundry.org/bytefmt v0.0.0-20200131002437-cf55d5288a48 // indirect
	github.com/99designs/keyring v1.1.5 // indirect
	github.com/BurntSushi/toml v0.3.1
	github.com/DataDog/zstd v1.4.6-0.20200617134701-89f69fb7df32 // indirect
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/apache/pulsar-client-go v0.1.1
	github.com/apache/pulsar/pulsar-client-go v0.0.0-20200901051823-800681aaa9af
	github.com/aws/aws-sdk-go v1.30.8
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f
	github.com/danieljoos/wincred v1.1.0 // indirect
	github.com/docker/go-units v0.4.0
	github.com/dvsekhvalnov/jose2go v0.0.0-20200901110807-248326c1351b // indirect
	github.com/frankban/quicktest v1.10.2 // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/go-kit/kit v0.10.0 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/golang/mock v1.4.4 // indirect
	github.com/golang/protobuf v1.4.2
	github.com/google/btree v1.0.0
	github.com/google/martian/v3 v3.0.0 // indirect
	github.com/google/pprof v0.0.0-20200708004538-1a94d8640e99 // indirect
	github.com/google/uuid v1.1.1
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/json-iterator/go v1.1.10
	github.com/julienschmidt/httprouter v1.3.0 // indirect
	github.com/keybase/go-keychain v0.0.0-20200502122510-cda31fe0c86d // indirect
	github.com/klauspost/compress v1.10.11 // indirect
	github.com/minio/minio-go/v7 v7.0.5
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/onsi/ginkgo v1.12.1 // indirect
	github.com/onsi/gomega v1.10.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.4 // indirect
	github.com/pingcap/log v0.0.0-20200828042413-fce0951f1463
	github.com/pivotal-golang/bytefmt v0.0.0-20200131002437-cf55d5288a48
	github.com/prometheus/client_golang v1.5.1 // indirect
	github.com/prometheus/common v0.10.0 // indirect
	github.com/prometheus/procfs v0.1.3 // indirect
	github.com/rs/xid v1.2.1
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.6.1
	github.com/tikv/client-go v0.0.0-20200824032810-95774393107b
	github.com/tikv/pd v2.1.19+incompatible
	github.com/yahoo/athenz v1.9.16 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.opencensus.io v0.22.4 // indirect
	go.uber.org/zap v1.15.0
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a // indirect
	golang.org/x/exp v0.0.0-20200224162631-6cc2880d07d6 // indirect
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/net v0.0.0-20200904194848-62affa334b73 // indirect
	golang.org/x/sys v0.0.0-20200905004654-be1d3432aa8f // indirect
	golang.org/x/tools v0.0.0-20200825202427-b303f430e36d // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/api v0.22.0 // indirect
	google.golang.org/appengine v1.6.6 // indirect
	google.golang.org/grpc v1.31.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.3.0
	honnef.co/go/tools v0.0.1-2020.1.4 // indirect
	k8s.io/utils v0.0.0-20200912215256-4140de9c8800 // indirect
	rsc.io/quote/v3 v3.1.0 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)

replace (
	github.com/coreos/etcd => github.com/ozonru/etcd v3.3.20-grpc1.27-origmodule+incompatible
	go.etcd.io/bbolt => go.etcd.io/bbolt v1.3.5

	google.golang.org/api => google.golang.org/api v0.14.0

	//replace go.etcd.io/etcd => go.etcd.io/etcd v0.5.0-alpha.5.0.20200329194405-dd816f0735f8
	google.golang.org/grpc => google.golang.org/grpc v1.26.0
)
