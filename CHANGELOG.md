# Changelog

Please mark all change in change log and use the ticket from JIRA.

# Milvus 0.5.1 (TODO)

## Bug
- \#134 - JFrog cache error

## Feature
- \#90 - The server start error messages could be improved to enhance user experience
- \#104 - test_scheduler core dump
- \#115 - Using new structure for tasktable
- \#139 - New config opion use_gpu_threshold
- \#146 - Add only GPU and only CPU version for IVF_SQ8 and IVF_FLAT

## Improvement
- \#64 - Improvement dump function in scheduler
- \#80 - Print version information into log during server start
- \#82 - Move easyloggingpp into "external" directory
- \#92 - Speed up CMake build process
- \#96 - Remove .a file in milvus/lib for docker-version
- \#118 - Using shared_ptr instead of weak_ptr to avoid performance loss
- \#122 - Add unique id for Job
- \#130 - Set task state MOVED after resource copy it completed
- \#149 - Improve large query optimizer pass

## Task

# Milvus 0.5.0 (2019-10-21)

## Bug
- MS-568 - Fix gpuresource free error
- MS-572 - Milvus crash when get SIGINT
- MS-577 - Unittest Query randomly hung
- MS-587 - Count get wrong result after adding vectors and index built immediately
- MS-599 - Search wrong result when table created with metric_type: IP
- MS-601 - Docker logs error caused by get CPUTemperature error
- MS-605 - Server going down during searching vectors
- MS-620 - Get table row counts display wrong error code
- MS-622 - Delete vectors should be failed if date range is invalid
- MS-624 - Search vectors failed if time ranges long enough
- MS-637 - Out of memory when load too many tasks
- MS-639 - SQ8H index created failed and server hang
- MS-640 - Cache object size calculate incorrect
- MS-641 - Segment fault(signal 11) in PickToLoad
- MS-644 - Search crashed with index-type: flat
- MS-647 - grafana display average cpu-temp
- MS-652 - IVFSQH quantization double free
- MS-650 - SQ8H index create issue
- MS-653 - When config check fail, Milvus close without message
- MS-654 - Describe index timeout when building index
- MS-658 - Fix SQ8 Hybrid can't search
- MS-665 - IVF_SQ8H search crash when no GPU resource in search_resources
- \#9 - Change default gpu_cache_capacity to 4
- \#20 - C++ sdk example get grpc error 
- \#23 - Add unittest to improve code coverage
- \#31 - make clang-format failed after run build.sh -l
- \#39 - Create SQ8H index hang if using github server version
- \#30 - Some troubleshoot messages in Milvus do not provide enough information
- \#48 - Config unittest failed
- \#59 - Topk result is incorrect for small dataset

## Improvement
- MS-552 - Add and change the easylogging library
- MS-553 - Refine cache code
- MS-555 - Remove old scheduler
- MS-556 - Add Job Definition in Scheduler
- MS-557 - Merge Log.h
- MS-558 - Refine status code
- MS-562 - Add JobMgr and TaskCreator in Scheduler
- MS-566 - Refactor cmake
- MS-574 - Milvus configuration refactor
- MS-578 - Make sure milvus5.0 don't crack 0.3.1 data
- MS-585 - Update namespace in scheduler
- MS-606 - Speed up result reduce
- MS-608 - Update TODO names
- MS-609 - Update task construct function
- MS-611 - Add resources validity check in ResourceMgr
- MS-619 - Add optimizer class in scheduler
- MS-626 - Refactor DataObj to support cache any type data
- MS-648 - Improve unittest
- MS-655 - Upgrade SPTAG
- \#42 - Put union of index_build_device and search resources to gpu_pool
- \#67 - Avoid linking targets multiple times in cmake

## Feature
- MS-614 - Preload table at startup
- MS-627 - Integrate new index: IVFSQHybrid
- MS-631 - IVFSQ8H Index support
- MS-636 - Add optimizer in scheduler for FAISS_IVFSQ8H

## Task
- MS-554 - Change license to Apache 2.0
- MS-561 - Add contributing guidelines, code of conduct and README docs
- MS-567 - Add NOTICE.md
- MS-569 - Complete the NOTICE.md
- MS-575 - Add Clang-format & Clang-tidy & Cpplint
- MS-586 - Remove BUILD_FAISS_WITH_MKL option
- MS-590 - Refine cmake code to support cpplint
- MS-600 - Reconstruct unittest code
- MS-602 - Remove zilliz namespace
- MS-610 - Change error code base value from hex to decimal
- MS-624 - Re-organize project directory for open-source
- MS-635 - Add compile option to support customized faiss
- MS-660 - add ubuntu_build_deps.sh
- \#18 - Add all test cases
	
# Milvus 0.4.0 (2019-09-12)

## Bug
- MS-119 - The problem of combining the log files
- MS-121 - The problem that user can't change the time zone
- MS-411 - Fix metric unittest linking error
- MS-412 - Fix gpu cache logical error
- MS-416 - ExecutionEngineImpl::GpuCache has not return value cause crash
- MS-417 - YAML sequence load disable cause scheduler startup failed
- MS-413 - Create index failed and server exited
- MS-427 - Describe index error after drop index
- MS-432 - Search vectors params nprobe need to check max number
- MS-431 - Search vectors params nprobe: 0/-1, expected result: raise exception
- MS-331 - Crate Table : when table exists, error code is META_FAILED(code=15) rather than ILLEGAL TABLE NAME(code=9))
- MS-430 - Search no result if index created with FLAT
- MS-443 - Create index hang again
- MS-436 - Delete vectors failed if index created with index_type: IVF_FLAT/IVF_SQ8
- MS-449 - Add vectors twice success, once with ids, the other no ids
- MS-450 - server hang after run stop_server.sh
- MS-458 - Keep building index for one file when no gpu resource
- MS-461 - Mysql meta unittest failed
- MS-462 - Run milvus server twices, should display error
- MS-463 - Search timeout
- MS-467 - mysql db test failed
- MS-470 - Drop index success, which table not created
- MS-471 - code coverage run failed
- MS-492 - Drop index failed if index have been created with index_type: FLAT
- MS-493 - Knowhere unittest crash
- MS-453 - GPU search error when nprobe set more than 1024
- MS-474 - Create index hang if use branch-0.3.1 server config
- MS-510 - unittest out of memory and crashed
- MS-507 - Dataset 10m-512, index type sq8，performance in-normal when set CPU_CACHE to 16 or 64
- MS-543 - SearchTask fail without exception
- MS-582 - grafana displays changes frequently

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
- MS-406 - Add table flag for meta
- MS-403 - Add GpuCacheMgr
- MS-404 - Release index after search task done avoid memory increment continues
- MS-405 - Add delete task support
- MS-407 - Reconstruct MetricsCollector
- MS-408 - Add device_id in resource construct function
- MS-409 - Using new scheduler
- MS-413 - Remove thrift dependency
- MS-410 - Add resource config comment
- MS-414 - Add TaskType in Scheduler::Task
- MS-415 - Add command tasktable to dump all tasktables
- MS-418 - Update server_config.template file, set CPU compute only default
- MS-419 - Move index_file_size from IndexParam to TableSchema
- MS-421 - Add TaskLabel in scheduler
- MS-422 - Support DeleteTask in Multi-GpuResource case
- MS-428 - Add PushTaskByDataLocality in scheduler
- MS-440 - Add DumpTaskTables in sdk
- MS-442 - Merge Knowhere
- MS-445 - Rename CopyCompleted to LoadCompleted
- MS-451 - Update server_config.template file, set GPU compute default
- MS-455 - Distribute tasks by minimal cost in scheduler
- MS-460 - Put transport speed as weight when choosing neighbour to execute task
- MS-459 - Add cache for pick function in tasktable
- MS-476 - Improve search performance
- MS-482 - Change search stream transport to unary in grpc
- MS-487 - Define metric type in CreateTable
- MS-488 - Improve code format in scheduler
- MS-495 - cmake: integrated knowhere
- MS-496 - Change the top_k limitation from 1024 to 2048
- MS-502 - Update tasktable_test in scheduler
- MS-504 - Update node_test in scheduler
- MS-505 - Install core unit test and add to coverage
- MS-508 - Update normal_test in scheduler
- MS-532 - Add grpc server unittest
- MS-511 - Update resource_test in scheduler
- MS-517 - Update resource_mgr_test in scheduler
- MS-518 - Add schedinst_test in scheduler
- MS-519 - Add event_test in scheduler
- MS-520 - Update resource_test in scheduler
- MS-524 - Add some unittest in event_test and resource_test
- MS-525 - Disable parallel reduce in SearchTask
- MS-527 - Update scheduler_test and enable it
- MS-528 - Hide some config used future
- MS-530 - Add unittest for SearchTask->Load
- MS-531 - Disable next version code
- MS-533 - Update resource_test to cover dump function
- MS-523 - Config file validation
- MS-539 - Remove old task code
- MS-546 - Add simple mode resource_config
- MS-570 - Add prometheus docker-compose file
- MS-576 - Scheduler refactor
- MS-592 - Change showtables stream transport to unary

## Feature
- MS-343 - Implement ResourceMgr
- MS-338 - NewAPI: refine code to support CreateIndex
- MS-339 - NewAPI: refine code to support DropIndex
- MS-340 - NewAPI: implement DescribeIndex

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
- MS-347 - Build index hangs again
- MS-382 - fix MySQLMetaImpl::CleanUpFilesWithTTL unknown column bug

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
- MS-352 - Add hybrid index

## Feature
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

## Feature

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

## Feature

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
