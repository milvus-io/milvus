# Changelog

Please mark all change in change log and use the ticket from JIRA.

# Milvus 0.4.0 (2019-07-28)

## Bug

## Improvement
- MS-327 - Clean code for milvus
- MS-336 - Scheduler interface
- MS-344 - Add TaskTable Test
- MS-345 - Add Node Test
- MS-346 - Add some implementation of scheduler to solve compile error
- MS-348 - Add ResourceFactory Test
- MS-350 - Remove knowhere submodule
- MS-354 - Add task class and interface in scheduler
- MS-355 - Add copy interface in ExcutionEngine
- MS-357 - Add minimum schedule function
- MS-359 - Add cost test in new scheduler
- MS-361 - Add event in resource
- MS-364 - Modify tasktableitem in tasktable
- MS-365 - Use tasktableitemptr instead in event
- MS-366 - Implement TaskTable
- MS-368 - Implement cost.cpp
- MS-371 - Add TaskTableUpdatedEvent
- MS-373 - Add resource test
- MS-374 - Add action definition
- MS-375 - Add Dump implementation for Event
- MS-376 - Add loader and executor enable flag in Resource avoid diskresource execute task
- MS-377 - Improve process thread trigger in ResourceMgr, Scheduler and TaskTable
- MS-378 - Debug and Update normal_test in scheduler unittest
- MS-379 - Add Dump implementation in Resource
- MS-380 - Update resource loader and executor, work util all finished
- MS-383 - Modify condition variable usage in scheduler
- MS-384 - Add global instance of ResourceMgr and Scheduler
- MS-389 - Add clone interface in Task
- MS-390 - Update resource construct function
- MS-391 - Add PushTaskToNeighbourHasExecutor action
- MS-394 - Update scheduler unittest
- MS-400 - Add timestamp record in task state change function
- MS-402 - Add dump implementation for TaskTableItem
- MS-404 - Release index after search task done avoid memory increment continues
- MS-405 - Add delete task support
- MS-408 - Add device_id in resource construct function

## New Feature
- MS-343 - Implement ResourceMgr

## Task
- MS-297 - disable mysql unit test

# Milvus 0.3.1 (2019-07-10)

## Bug

- MS-148 - Disable cleanup if mode is read only
- MS-149 - Fixed searching only one index file issue in distributed mode
- MS-153 - Fix c_str error when connecting to MySQL
- MS-157 - Fix changelog
- MS-190 - Use env variable to switch mem manager and fix cmake
- MS-217 - Fix SQ8 row count bug
- MS-224 - Return AlreadyExist status in MySQLMetaImpl::CreateTable if table already exists
- MS-232 - Add MySQLMetaImpl::UpdateTableFilesToIndex and set maximum_memory to default if config value = 0
- MS-233 - Remove mem manager log
- MS-230 - Change parameter name: Maximum_memory to insert_buffer_size
- MS-234 - Some case cause background merge thread stop
- MS-235 - Some test cases random fail
- MS-236 - Add MySQLMetaImpl::HasNonIndexFiles
- MS-257 - Update bzip2 download url
- MS-288 - Update compile scripts
- MS-330 - Stability test failed caused by server core dumped

## Improvement
- MS-156 - Add unittest for merge result functions
- MS-152 - Delete assert in MySQLMetaImpl and change MySQLConnectionPool impl
- MS-204 - Support multi db_path
- MS-206 - Support SQ8 index type
- MS-208 - Add buildinde interface for C++ SDK
- MS-212 - Support Inner product metric type
- MS-241 - Build Faiss with MKL if using Intel CPU; else build with OpenBlas
- MS-242 - Clean up cmake and change MAKE_BUILD_ARGS to be user defined variable
- MS-245 - Improve search result transfer performance
- MS-248 - Support AddVector/SearchVector profiling
- MS-256 - Add more cache config
- MS-260 - Refine log
- MS-249 - Check machine hardware during initialize
- MS-261 - Update faiss version to 1.5.3 and add BUILD_FAISS_WITH_MKL as an option
- MS-266 - Improve topk reduce time by using multi-threads
- MS-275 - Avoid sqlite logic error excetion
- MS-278 - add IndexStatsHelper
- MS-313 - add GRPC
- MS-325 - add grpc status return for C++ sdk and modify some format
- MS-278 - Add IndexStatsHelper
- MS-312 - Set openmp thread number by config
- MS-305 - Add CPU core percent metric
- MS-310 - Add milvus CPU utilization ratio and CPU/GPU temperature metrics
- MS-324 - Show error when there is not enough gpu memory to build index
- MS-328 - Check metric type on server start
- MS-332 - Set grpc and thrift server run concurrently

## New Feature
- MS-180 - Add new mem manager
- MS-195 - Add nlist and use_blas_threshold conf
- MS-137 - Integrate knowhere

## Task

- MS-125 - Create 0.3.1 release branch
- MS-306 - Optimize build efficiency

# Milvus 0.3.0 (2019-06-30)

## Bug
- MS-104 - Fix unittest lcov execution error
- MS-102 - Fix build script file condition error
- MS-80 - Fix server hang issue
- MS-89 - Fix compile failed, libgpufaiss.a link missing
- MS-90 - Fix arch match incorrect on ARM
- MS-99 - Fix compilation bug
- MS-110 - Avoid huge file size

## Improvement
- MS-82 - Update server startup welcome message
- MS-83 - Update vecwise to Milvus
- MS-77 - Performance issue of post-search action
- MS-22 - Enhancement for MemVector size control 
- MS-92 - Unify behavior of debug and release build
- MS-98 - Install all unit test to installation directory
- MS-115 - Change is_startup of metric_config switch from true to on
- MS-122 - Archive criteria config 
- MS-124 - HasTable interface
- MS-126 - Add more error code
- MS-128 - Change default db path

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
- MS-85 - add NetIO metric
- MS-96 - add new query interface for specified files
- MS-97 - Add S3 SDK for MinIO Storage
- MS-105 - Add MySQL
- MS-130 - Add prometheus_test
- MS-144 - Add nprobe config
- MS-147 - Enable IVF

- MS-130 - Add prometheus_test
## Task
- MS-74 - Change README.md in cpp
- MS-88 - Add support for arm architecture

# Milvus 0.2.0 (2019-05-31)

## Bug

- MS-32 - Fix thrift error
- MS-34 - Fix prometheus-cpp thirdparty
- MS-67 - Fix license check bug
- MS-76 - Fix pipeline crash bug
- MS-100 - cmake: fix AWS build issue
- MS-101 - change AWS build type to Release

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
