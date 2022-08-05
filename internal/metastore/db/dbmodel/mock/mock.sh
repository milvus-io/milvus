mockgen -source=internal/metastore/db/dbmodel/collection.go -destination=internal/metastore/db/dbmodel/mock/collection_mock.go -package=mock
mockgen -source=internal/metastore/db/dbmodel/collection_alias.go -destination=internal/metastore/db/dbmodel/mock/collection_alias_mock.go -package=mock
mockgen -source=internal/metastore/db/dbmodel/collection_channel.go -destination=internal/metastore/db/dbmodel/mock/collection_channel_mock.go -package=mock
mockgen -source=internal/metastore/db/dbmodel/field.go -destination=internal/metastore/db/dbmodel/mock/field_mock.go -package=mock
mockgen -source=internal/metastore/db/dbmodel/index.go -destination=internal/metastore/db/dbmodel/mock/index_mock.go -package=mock
mockgen -source=internal/metastore/db/dbmodel/partition.go -destination=internal/metastore/db/dbmodel/mock/partition_mock.go -package=mock
mockgen -source=internal/metastore/db/dbmodel/segment_index.go -destination=internal/metastore/db/dbmodel/mock/segment_index_mock.go -package=mock
mockgen -source=internal/metastore/db/dbmodel/user.go -destination=internal/metastore/db/dbmodel/mock/user_mock.go -package=mock