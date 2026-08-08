# Archived Milvus developer guides

These documents describe early Milvus 2.0 architecture and superseded
development workflows. They are retained for historical context and must not
be treated as instructions for the current codebase.

## Architecture chapters

- [System overview](chap01_system_overview.md)
- [Schema](chap02_schema.md)
- [Index service](chap03_index_service.md)
- [Message stream](chap04_message_stream.md)
- [Proxy](chap05_proxy.md)
- [Root coordinator](chap06_root_coordinator.md)
- [Query coordinator](chap07_query_coordinator.md)
- [Binlog](chap08_binlog.md)
- [Data coordinator](chap09_data_coord.md)

## Appendices and supporting notes

- [Basic components](appendix_a_basic_components.md)
- [API reference](appendix_b_api_reference.md)
- [System configurations](appendix_c_system_configurations.md)
- [Error codes](appendix_d_error_code.md)
- [Guarantee timestamp](how-guarantee-ts-works.md)
- [Guarantee timestamp (Chinese)](how-guarantee-ts-works-cn.md)
- [Proxy result reduction](proxy-reduce.md)
- [Proxy result reduction (Chinese)](proxy-reduce-cn.md)
- [Original acknowledgements](developer_guides.md)

## Superseded development workflow

- [Developing with a local milvus-proto checkout](how_to_develop_with_local_milvus_proto.md)

The local `milvus-proto` guide is archived because it requires directly
editing `scripts/generate_proto.sh` and contains generated-code examples that
no longer match the repository. Do not follow it without first validating the
workflow against the current build scripts.
