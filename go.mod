module github.com/milvus-io/milvus

go 1.22

toolchain go1.22.0

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.11.1
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.6.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.1.0
	github.com/aliyun/credentials-go v1.2.7
	github.com/apache/arrow/go/v12 v12.0.1
	github.com/bits-and-blooms/bloom/v3 v3.0.1
	github.com/blang/semver/v4 v4.0.0
	github.com/casbin/casbin/v2 v2.44.2
	github.com/casbin/json-adapter/v2 v2.0.0
	github.com/cockroachdb/errors v1.9.1
	github.com/gin-gonic/gin v1.9.1
	github.com/go-playground/validator/v10 v10.14.0
	github.com/gofrs/flock v0.8.1
	github.com/golang/protobuf v1.5.4
	github.com/google/btree v1.1.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/klauspost/compress v1.17.9
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d
	github.com/milvus-io/milvus-proto/go-api/v2 v2.3.4-0.20240930043709-0c23514e4c34
	github.com/minio/minio-go/v7 v7.0.73
	github.com/pingcap/log v1.1.1-0.20221015072633-39906604fb81
	github.com/prometheus/client_golang v1.14.0
	github.com/prometheus/client_model v0.3.0
	github.com/prometheus/common v0.42.0
	github.com/quasilyte/go-ruleguard/dsl v0.3.22
	github.com/samber/lo v1.27.0
	github.com/sbinet/npyio v0.6.0
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cast v1.3.1
	github.com/spf13/viper v1.8.1
	github.com/stretchr/testify v1.9.0
	github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common v1.0.865
	github.com/tikv/client-go/v2 v2.0.4
	go.etcd.io/etcd/api/v3 v3.5.5
	go.etcd.io/etcd/client/v3 v3.5.5
	go.etcd.io/etcd/server/v3 v3.5.5
	go.opentelemetry.io/otel v1.28.0
	go.opentelemetry.io/otel/trace v1.28.0
	go.uber.org/atomic v1.11.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.0
	golang.org/x/crypto v0.25.0
	golang.org/x/exp v0.0.0-20240506185415-9bf2ced13842
	golang.org/x/net v0.27.0
	golang.org/x/oauth2 v0.21.0
	golang.org/x/sync v0.7.0
	golang.org/x/text v0.16.0
	google.golang.org/grpc v1.65.0
	google.golang.org/grpc/examples v0.0.0-20220617181431-3e7b97febc7f
)

require (
	cloud.google.com/go/storage v1.43.0
	github.com/antlr4-go/antlr/v4 v4.13.1
	github.com/bits-and-blooms/bitset v1.10.0
	github.com/bytedance/sonic v1.12.2
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/cockroachdb/redact v1.1.3
	github.com/goccy/go-json v0.10.3
	github.com/greatroar/blobloom v0.0.0-00010101000000-000000000000
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/jolestar/go-commons-pool/v2 v2.1.2
	github.com/milvus-io/milvus/pkg v0.0.2-0.20240801085213-a642a26ed4c6
	github.com/pkg/errors v0.9.1
	github.com/remeh/sizedwaitgroup v1.0.0
	github.com/tidwall/gjson v1.17.1
	github.com/valyala/fastjson v1.6.4
	github.com/zeebo/xxh3 v1.0.2
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0
	google.golang.org/api v0.187.0
	google.golang.org/protobuf v1.34.2
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cloud.google.com/go v0.115.0 // indirect
	cloud.google.com/go/auth v0.6.1 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.2 // indirect
	cloud.google.com/go/compute/metadata v0.3.0 // indirect
	cloud.google.com/go/iam v1.1.8 // indirect
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4 // indirect
	github.com/99designs/keyring v1.2.1 // indirect
	github.com/AthenZ/athenz v1.10.39 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.8.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.2.2 // indirect
	github.com/DataDog/zstd v1.5.0 // indirect
	github.com/JohnCGriffin/overflow v0.0.0-20211019200055-46fa312c352c // indirect
	github.com/Knetic/govaluate v3.0.1-0.20171022003610-9aa49832a739+incompatible // indirect
	github.com/alibabacloud-go/debug v0.0.0-20190504072949-9472017b5c68 // indirect
	github.com/alibabacloud-go/tea v1.1.8 // indirect
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/apache/pulsar-client-go v0.6.1-0.20210728062540-29414db801a7 // indirect
	github.com/apache/thrift v0.18.1 // indirect
	github.com/ardielle/ardielle-go v1.5.2 // indirect
	github.com/benesch/cgosymbolizer v0.0.0-20190515212042-bec6fe6e597b // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bytedance/sonic/loader v0.2.0 // indirect
	github.com/campoy/embedmd v1.0.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cilium/ebpf v0.11.0 // indirect
	github.com/cloudwego/base64x v0.1.4 // indirect
	github.com/cloudwego/iasm v0.2.0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20211118104740-dabe8e521a4f // indirect
	github.com/confluentinc/confluent-kafka-go v1.9.1 // indirect
	github.com/containerd/cgroups/v3 v3.0.3 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/danieljoos/wincred v1.1.2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/dvsekhvalnov/jose2go v1.6.0 // indirect
	github.com/expr-lang/expr v1.15.7 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/form3tech-oss/jwt-go v3.2.3+incompatible // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/gabriel-vasile/mimetype v1.4.2 // indirect
	github.com/getsentry/sentry-go v0.12.0 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/godbus/dbus/v5 v5.0.4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang-jwt/jwt/v5 v5.2.1 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v2.0.8+incompatible // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.5 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.16.0 // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/ianlancetaylor/cgosymbolizer v0.0.0-20221217025313-27d3c9f66b6a // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/leodido/go-urn v1.2.4 // indirect
	github.com/linkedin/goavro/v2 v2.11.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magiconair/properties v1.8.5 // indirect
	github.com/mattn/go-colorable v0.1.11 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/nats-io/jwt/v2 v2.5.5 // indirect
	github.com/nats-io/nats-server/v2 v2.10.12 // indirect
	github.com/nats-io/nats.go v1.34.1 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/opencontainers/runtime-spec v1.0.2 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/panjf2000/ants/v2 v2.7.2 // indirect
	github.com/pelletier/go-toml v1.9.3 // indirect
	github.com/pelletier/go-toml/v2 v2.0.8 // indirect
	github.com/petermattis/goid v0.0.0-20180202154549-b0b1615b78e5 // indirect
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c // indirect
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00 // indirect
	github.com/pingcap/goleveldb v0.0.0-20191226122134-f82aafb29989 // indirect
	github.com/pingcap/kvproto v0.0.0-20221129023506-621ec37aac7a // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rogpeppe/go-internal v1.12.0 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/sasha-s/go-deadlock v0.3.1 // indirect
	github.com/shirou/gopsutil/v3 v3.22.9 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/afero v1.6.0 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stathat/consistent v1.0.0 // indirect
	github.com/streamnative/pulsarctl v0.5.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c // indirect
	github.com/tiancaiamao/gp v0.0.0-20221230034425-4025bc8a4d4a // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tikv/pd/client v0.0.0-20221031025758-80f0d8ca4d07 // indirect
	github.com/tklauser/go-sysconf v0.3.10 // indirect
	github.com/tklauser/numcpus v0.4.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20201229170055-e5319fda7802 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/twmb/murmur3 v1.1.3 // indirect
	github.com/uber/jaeger-client-go v2.30.0+incompatible // indirect
	github.com/ugorji/go/codec v1.2.11 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.5 // indirect
	go.etcd.io/etcd/client/v2 v2.305.5 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.5 // indirect
	go.etcd.io/etcd/raft/v3 v3.5.5 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/otel/exporters/jaeger v1.13.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.20.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.20.0 // indirect
	go.opentelemetry.io/otel/metric v1.28.0 // indirect
	go.opentelemetry.io/otel/sdk v1.28.0 // indirect
	go.opentelemetry.io/proto/otlp v1.0.0 // indirect
	go.uber.org/automaxprocs v1.5.3 // indirect
	golang.org/x/arch v0.3.0 // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/sys v0.22.0 // indirect
	golang.org/x/term v0.22.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	gonum.org/v1/gonum v0.11.0 // indirect
	google.golang.org/genproto v0.0.0-20240624140628-dc46fd24d27d // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240617180043-68d350f18fd4 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240730163845-b1a4ccb954bf // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/apimachinery v0.28.6 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)

replace (
	github.com/apache/arrow/go/v12 => github.com/milvus-io/arrow/go/v12 v12.0.1
	github.com/apache/pulsar-client-go => github.com/milvus-io/pulsar-client-go v0.12.1
	github.com/bketelsen/crypt => github.com/bketelsen/crypt v0.0.4 // Fix security alert for core-os/etcd
	github.com/expr-lang/expr => github.com/SimFG/expr v0.0.0-20241012101405-9f20ab151517
	github.com/go-kit/kit => github.com/go-kit/kit v0.1.0
	github.com/greatroar/blobloom => github.com/milvus-io/blobloom v0.0.0-20240603110411-471ae49f3b93
	github.com/ianlancetaylor/cgosymbolizer => github.com/milvus-io/cgosymbolizer v0.0.0-20240722103217-b7dee0e50119
	github.com/milvus-io/milvus/pkg => ./pkg
	github.com/streamnative/pulsarctl => github.com/xiaofan-luan/pulsarctl v0.5.1
	github.com/tecbot/gorocksdb => github.com/milvus-io/gorocksdb v0.0.0-20220624081344-8c5f4212846b // indirect
)

exclude github.com/apache/pulsar-client-go/oauth2 v0.0.0-20211108044248-fe3b7c4e445b
