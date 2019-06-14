# Changelog

Please mark all change in change log and use the ticket from JIRA.

# MegaSearch 0.3.0 (TBD)

## Bug
- MS-80 - Fix server hang issue

## Improvement
- MS-82 - Update server startup welcome message
- MS-83 - Update vecwise to Milvus
- MS-77 - Performance issue of post-search action

## New Feature

- MS-57 - Implement index load/search pipeline
- MS-56 - Add version information when server is started
- MS-64 - Different table can have different index type
- MS-52 - Return search score
- MS-66 - Support time range query
- MS-68 - Remove rocksdb from third-party
- MS-70 - cmake: remove redundant libs in src
- MS-71 - cmake: fix faiss dependency
- MS-72 - cmake: change prometheus source to git
- MS-73 - cmake: delete civetweb
- MS-65 - Implement GetTableRowCount interface
- MS-45 - Implement DeleteTable interface
- MS-75 - cmake: change faiss version to 1.5.2; add CUDA gencode
- MS-81 - fix faiss ptx issue; change cuda gencode
- MS-84 - cmake: add arrow, jemalloc and jsoncons third party; default build option OFF

## Task
- MS-74 - Change README.md in cpp

# MegaSearch 0.2.0 (2019-05-31)

## Bug

- MS-32 - Fix thrift error
- MS-34 - Fix prometheus-cpp thirdparty
- MS-67 - Fix license check bug
- MS-76 - Fix pipeline crash bug

## Improvement

- MS-20 - Clean Code Part 1

## New Feature

- MS-5 - Implement Auto Archive Feature
- MS-6 - Implement SDK interface part 1
- MS-16 - Implement metrics without prometheus
- MS-21 - Implement SDK interface part 2
- MS-26 - cmake. Add thirdparty packages
- MS-31 - cmake: add prometheus
- MS-33 - cmake: add -j4 to make third party packages build faster
- MS-27 - support gpu config and disable license build config in cmake
- MS-47 - Add query vps metrics
- MS-37 - Add query, cache usage, disk write speed and file data size metrics
- MS-30 - Use faiss v1.5.2
- MS-54 - cmake: Change Thrift third party URL to github.com
- MS-69 - prometheus: add all proposed metrics

## Task

- MS-1 - Add CHANGELOG.md
- MS-4 - Refactor the vecwise_engine code structure
- MS-62 - Search range to all if no date specified
