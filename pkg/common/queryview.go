package common

// QueryViewSchemaProtocol identifies exact-schema serving, publication fencing,
// and view-owned runtime resources. It is independent of the release version.
const QueryViewSchemaProtocol = "1"

// QueryViewSchemaProtocolHeader attests the protocol on each worker sync stream,
// including streams reopened after node replacement or pchannel migration.
const QueryViewSchemaProtocolHeader = "milvus-queryview-schema-protocol"
