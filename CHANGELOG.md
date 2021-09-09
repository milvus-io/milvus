# Changelog

Please mark all change in change log and use the issue from GitHub

# Milvus 1.1.0 (2021-06-16)
## Bug
-   \#4897 Query results contain some deleted ids.
-   \#5164 Exception should be raised if insert or delete entity on the none-existed partition.
-   \#5191 Mishards throw "index out of range" error after continually search/insert for a period of time.
-   \#5398 Random crash after request is executed.
-   \#5537 Failed to load bloom filter after suddenly power off.
-   \#5574 IVF_SQ8 and IVF_PQ cannot be built on multiple GPUs.
-   \#5747 Search with big nq and topk crash milvus.
  
## Features
-   \#1434 Storage: enabling s3 storage support (implemented by Unisinsight).
-   \#5142 Support keeping index in GPU memory.

## Improvements
-   \#5115 Relax the topk limit from 16384 to 1M for CPU search.
-   \#5204 Improve IVF query on GPU when no entity deleted.
-   \#5544 Relax the index_file_size limit from 4GB to 128Gb.


## Feature
-   \#3977 Support logging to stdout

## Improvement
-   \#4754 Reduce the package download size of grpc-milvus for C++ SDK

## Task

# Milvus 0.10.6 (2021-02-23)
## Bug
-   \#4683 A negative zero may be returned if the metric_type is Tanimoto
-   \#4678 Server crash on BinaryFlat if dimension is not a power of 2
-   \#4719 The GPU cache holds much more data than the configured value

## Feature

## Improvement
-   \#1970 Improve the performance of BinaryFlat by AVX2
-   \#3920 Add an optional parameter 'nbtis' for IVF_PQ
-   \#4676,#4614 Support configurable metric labels cluster and instance for Prometheus (implemented by IQIYI)

## Task

# Milvus 0.10.5 (2021-01-07)
## Bug
-   \#4296,#4554 Fix mishards add name-mismatched pod to read-only group
-   \#4307 Specify partition to load for load_collection()
-   \#4378 Multi-threads to call load_collection() and search() cause Milvus hang
-   \#4484 Milvus only search default partition if search parameter 'partition_tags' contains '_default'
-   \#4492 while share file system servcie became abnormal, the milvus can not supply query service

## Feature
-   \#4504 Add a metric to display the number of files opened by Milvus

## Improvement
-   \#4454 Optimize the process of indexing and querying

## Task

# Milvus 0.10.4 (2020-12-02)
## Bug
-   \#3626 Fix server crash when searching with IVF_PQ on GPU
-   \#3903 The performance of IVF_SQ8H in 0.10.3 is degraded
-   \#3906 Fix the delete task state to avoid server crash
-   \#4012 Milvus hangs when continually creating and dropping partitions
-   \#4174 Fix out of memory caused by too many data loaded to GPU
-   \#4318 Fix memory leak in IVF indexes

## Feature
-   \#3773 Support IVF_PQ to run on FPGA (implemented by Montage-tech)

## Improvement
-   \#3775 Improve search performance in the case that no item deleted
-   \#4075 Improve performance for create large amount of partitions

## Task

# Milvus 0.10.3 (2020-9-21)
## Bug
-   \#3536 Release search task in time to avoid excessive memory usage
-   \#3656 Fix to check search params 'nprobe' of BIN_IVF_FLAT
-   \#3742 If the GPU cache is too small, IVF_SQ8H using multiple GPUs will cause to crash
-   \#3760 After IVF_SQ8H querying, an CUDA error will occur when Milvus exits

## Feature

## Improvement
-   \#3213 Allow users to specify a distance type at runtime for Flat index
-   \#3254 Allow more choices for the parameter ‘m’ of IVF_PQ
-   \#3606 The supported parameter 'nprobe' of IVF is expanded to [1, 16384]
-   \#3639 The supported parameter 'topk' of searching is expanded to [1, 16384]
-   \#3784 Upgrade mishards up to v0.10.3

## Task

# Milvus 0.10.2 (2020-08-15)

## Bug
-   \#2890 Fix the index size caculation in cache
-   \#2952 Fix the result merging of IVF_PQ IP
-   \#2975 Fix config UT failed
-   \#3012 If the cache is too small, queries using multiple GPUs will cause to crash
-   \#3133 Reverse query result in mishards if metric type is IP

## Feature

## Improvement
-   \#2653 Improve IVF search performance when NQ and nProbe are both large
-   \#2828 Let Faiss not to compile half float by default

## Task

# Milvus 0.10.1 (2020-07-20)

## Bug
-   \#2487 Enlarge timeout value for creating collection
-   \#2487 HotFix release lock failed on NAS
-   \#2557 Fix random crash of INSERT_DUPLICATE_ID case
-   \#2578 Result count doesn't match target vectors count
-   \#2585 Support IVF_PQ IP on GPU
-   \#2598 Fix Milvus docker image report illegal instruction
-   \#2617 Fix HNSW and RNSG index files size
-   \#2637 Suit the range of HNSW parameters
-   \#2642 Create index failed and server crashed
-   \#2649 Search parameter of annoy has conflict with document
-   \#2690 Remove body parser in show-partitions endpoints
-   \#2692 Milvus hangs during multi-thread concurrent search
-   \#2739 Fix mishards start failed
-   \#2752 Milvus formats vectors data to double-precision and return to http client
-   \#2767 Fix a bug of getting wrong nprobe limitation in knowhere on GPU version
-   \#2768 After building the index, the number of vectors increases
-   \#2774 Server down during loading data
-   \#2776 Fix too many data copies during creating IVF index
-   \#2813 To implemente RNSG IP

## Feature

## Improvement
-   \#2932 Upgrade mishards for milvus 0.10.1

## Task

# Milvus 0.10.0 (2020-06-15)

## Bug
-   \#2367 Fix inconsistent reading and writing when using mishards
-   \#2368 Make read node detect delete behavior
-   \#2394 Drop collection timeout if too many partitions created on collection
-   \#2549 Launch server fail using demo config
-   \#2564 cache.cache_size range check error

## Feature
-   \#2363 Update branch version
-   \#2510 Upgrade Milvus config

## Improvement
-   \#2381 Upgrade FAISS to 1.6.3
-   \#2429 Fix Milvus 0.9.1 performance degrade issue
-   \#2441 Improve Knowhere code coverage
-   \#2466 Optimize k-selection implemention of faiss gpu version
-   \#2489 Add exception throw on mysql meta error
-   \#2495 Add creating lock file failure reason.
-   \#2516 Improve unit test coverage
-   \#2548 Upgrade mishards for milvus v0.10.0

## Task

# Milvus 0.9.1 (2020-05-29)

## Bug
-   \#2366 Reduce SQL execution times for collection contains lot of partitions
-   \#2378 Duplicate data after server restart
-   \#2395 Fix large nq cudaMalloc error
-   \#2399 The nlist set by the user may not take effect
-   \#2403 MySQL max_idle_time is 10 by default
-   \#2450 The deleted vectors may be found on GPU
-   \#2456 openblas library install failed

## Feature

## Improvement
-   \#2353 Remove log_config from code and scripts
-   \#2370 Clean compile warning
-   \#2410 Logging build index progress
-   \#2461 Upgrade mishards for milvus 0.9.1

# Milvus 0.9.0 (2020-05-15)

## Bug
-   \#1705 Limit the insert data batch size
-   \#1776 Error out when index SQ8H run in CPU mode
-   \#1925 To flush all collections, flush cannot work
-   \#1929 Skip MySQL meta schema field width check
-   \#1946 Fix load index file CPU2GPU fail during searching
-   \#1955 Switch create_index operation to background once client break connection
-   \#1997 Index file missed after compact
-   \#2002 Remove log error msg `Attributes is null`
-   \#2073 Fix CheckDBConfigBackendUrl error message
-   \#2076 CheckMetricConfigAddress error message
-   \#2120 Fix Search expected failed if search params set invalid
-   \#2121 Allow regex match partition tag when search
-   \#2128 Check has_partition params
-   \#2131 Distance/ID returned is not correct if searching with duplicate ids
-   \#2141 Fix server start failed if wal directory exist
-   \#2169 Fix SingleIndexTest.IVFSQHybrid unittest
-   \#2194 Fix get collection info failed
-   \#2196 Fix server start failed if wal is disabled
-   \#2203 0.8.0 id=-1 is returned when total count < topk
-   \#2228 Fix show partitions failed in http module
-   \#2231 Use server_config to define hard-delete delay time for segment files
-   \#2261 Re-define result returned by has_collection if collection in delete state
-   \#2264 Milvus opened too many files when the metric_config.enable_monitor=true
-   \#2266 Server hangs when using multi-clients to query different collections
-   \#2280 has_partition should return true for `_default`

## Feature
-   \#1751 Add api SearchByID
-   \#1752 Add api GetVectorsByID
-   \#1962 Add api HasPartition
-   \#1965 FAISS/NSG/HNSW/ANNOY use unified distance calculation algorithm
-   \#2054 Check if CPU instruction sets are illegal
-   \#2057 Add a config parameter to switch off http server
-   \#2059 Add lock file avoid multiple instances modifying data at the same time
-   \#2064 Warn when use SQLite as metadata management
-   \#2111 Check GPU environment before start server
-   \#2206 Log file rotating
-   \#2240 Obtain running rpc requests information
-   \#2268 Intelligently detect openblas library in system to avoid installing from source code every time
-   \#2283 Suspend the building tasks when any query comand arrives.

## Improvement
-   \#221 Refactor LOG macro
-   \#833 Catch exception in RolloutHandler and output in stderr
-   \#1796 Compile Openblas with source code to improve the performance
-   \#1942 Background merge file strategy
-   \#2039 Support Milvus run on SSE CPUs
-   \#2149 Merge server_cpu_config.template and server_gpu_config.template
-   \#2153 Upgrade thirdparty oatpp to v1.0.0
-   \#2167 Merge log_config.conf with server_config.yaml
-   \#2173 Check storage permission
-   \#2178 Using elkan K-Means to improve IVF
-   \#2185 Change id to string format in http module
-   \#2186 Update endpoints in http module
-   \#2190 Fix memory usage is twice of index size when using GPU searching
-   \#2248 Use hostname and port as instance label of metrics
-   \#2252 Upgrade mishards APIs and requirements
-   \#2256 k-means clustering algorithm use only Euclidean distance metric
-   \#2300 Upgrade mishrads configuration to version 0.4
-   \#2311 Update mishards methods 
-   \#2330 Change url for behavior 'get_entities_by_id'
-   \#2347 Update http document for v0.9.0
-   \#2358 Upgrade mishards for v0.9.0 

## Task

# Milvus 0.8.0 (2020-04-15)

## Bug
-   \#1276 SQLite throw exception after create 50000+ partitions in a table
-   \#1762 Server is not forbidden to create new partition which tag is `_default`
-   \#1789 Fix multi-client search cause server crash
-   \#1832 Fix crash in tracing module
-   \#1873 Fix index file serialize to incorrect path
-   \#1881 Fix bad alloc when index files lost
-   \#1883 Fix inserted vectors becomes all zero when index_file_size >= 2GB
-   \#1901 Search failed with flat index
-   \#1903 Fix invalid annoy result
-   \#1910 C++ SDK GetIDsInSegment could not work for large dataset

## Feature
-   \#261  Integrate ANNOY into Milvus
-   \#1655 GPU index support delete vectors
-   \#1660 IVF PQ CPU support deleted vectors searching
-   \#1661 HNSW support deleted vectors searching
-   \#1825 Add annoy index type in C++ sdk
-   \#1849 NSG support deleted vectors searching
-   \#1893 Log config information and device information

## Improvement
-   \#1627 Move read/write index APIs into codec
-   \#1784 Add Substructure and Superstructure in http module
-   \#1858 Disable S3 build
-   \#1882 Add index annoy into http module
-   \#1885 Optimize knowhere unittest
-   \#1886 Refactor log on search and insert request
-   \#1897 Heap pop and push can be realized by heap_swap_top
-   \#1921 Use TimeRecorder instead of chrono
-   \#1928 Fix too many data and uid copies when loading files
-   \#1930 Upgrade mishards to v0.8.0

## Task

# Milvus 0.7.1 (2020-03-29)

## Bug
-   \#1301 Data in WAL may be accidentally inserted into a new table with the same name.
-   \#1634 Fix search demo bug in HTTP doc
-   \#1635 Vectors can be returned by searching after vectors deleted if `cache_insert_data` set true
-   \#1648 The cache cannot be used all when the vector type is binary
-   \#1651 Check validity of dimension when collection metric type is binary one
-   \#1663 PQ index parameter 'm' validation
-   \#1686 API search_in_files cannot work correctly when vectors is stored in certain non-default partition
-   \#1689 Fix SQ8H search fail on SIFT-1B dataset
-   \#1667 Create index failed with type: rnsg if metric_type is IP
-   \#1708 NSG search crashed
-   \#1724 Remove unused unittests
-   \#1728 Optimize request handler to combine similar query
-   \#1734 Opentracing for combined search request
-   \#1735 Fix search out of memory with ivf_flat 
-   \#1747 Expected error status if search with partition_tag not existed
-   \#1756 Fix memory exhausted during searching 
-   \#1781 Fix search hang with SQ8H
-   \#1812 Fix incorrect request method in search example in http readme
-   \#1818 Duplicate data generated after restart milvus server

## Feature
-   \#1603 BinaryFlat add 2 Metric: Substructure and Superstructure

## Improvement
-   \#267 Improve search performance: reduce delay
-   \#342 Knowhere and Wrapper refactor
-   \#1537 Optimize raw vector and uids read/write
-   \#1546 Move Config.cpp to config directory
-   \#1547 Rename storage/file to storage/disk and rename classes
-   \#1548 Move store/Directory to storage/Operation and add FSHandler
-   \#1572 Optimize config cpu/gpu cache_capacity setter
-   \#1619 Improve compact performance
-   \#1649 Fix Milvus crash on old CPU 
-   \#1653 IndexFlat (SSE) and IndexBinaryFlat performance improvement for small NQ
-   \#1678 Remove CUSTOMIZATION macro 
-   \#1698 Upgrade mishards to v0.7.0
-   \#1719 Improve Milvus log
-   \#1754 Optimize behavior to get file ids from metadata in mishards
-   \#1799 Update docker images to 0.7.1 in mishards

## Task

# Milvus 0.7.0 (2020-03-11)

## Bug
-   \#715 Milvus crash when searching and building index simultaneously using SQ8H
-   \#744 Don't return partition table for show_tables
-   \#770 Server unittest run failed on low-end server
-   \#805 IVFTest.gpu_seal_test unittest failed
-   \#831 Judge branch error in CommonUtil.cpp
-   \#977 Server crash when create tables concurrently
-   \#990 Check gpu resources setting when assign repeated value
-   \#995 Table count set to 0 if no tables found
-   \#1010 Improve error message when offset or page_size is equal 0
-   \#1022 Check if partition name is valid
-   \#1028 Check if table exists when show partitions
-   \#1029 Check if table exists when try to delete partition
-   \#1066 Optimize http insert and search speed
-   \#1022 Check if partition name is legal
-   \#1028 Check if table exists when show partitions
-   \#1029 Check if table exists when try to delete partition
-   \#1066 Optimize http insert and search speed
-   \#1067 Add binary vectors support in http server
-   \#1075 Improve error message when page size or offset is illegal
-   \#1082 Check page_size or offset value to avoid float
-   \#1115 Http server support load table into memory
-   \#1152 Error log output continuously after server start
-   \#1211 Server down caused by searching with index_type: HNSW
-   \#1240 Update license declaration
-   \#1298 Unit test failed when on CPU2GPU case
-   \#1359 Negative distance value returned when searching with HNSW index type
-   \#1429 Server crashed when searching vectors with GPU
-   \#1476 Fix vectors results bug when getting vectors from segments
-   \#1484 Index type changed to IDMAP after compacted
-   \#1491 Server crashed during adding vectors
-   \#1499 Fix duplicated ID number issue
-   \#1504 Avoid possible race condition between delete and search
-   \#1507 set_config for insert_buffer_size is wrong
-   \#1510 Add set interfaces for WAL configurations
-   \#1511 Fix big integer cannot pass to server correctly
-   \#1517 Result is not correct when search vectors in multi partition, index type is RNSG 
-   \#1518 Table count did not match after deleting vectors and compact
-   \#1521 Make cache_insert_data take effect in-service
-   \#1525 Add setter API for config preload_table
-   \#1529 Fix server crash when cache_insert_data enabled
-   \#1530 Set table file with correct engine type in meta
-   \#1532 Search with ivf_flat failed with open-dataset: sift-256-hamming
-   \#1535 Degradation searching performance with metric_type: binary_idmap
-   \#1549 Fix server/wal config setting bug
-   \#1556 Index file not created after table and index created
-   \#1560 Search crashed with Super-high dimensional binary vector
-   \#1564 Too low recall for glove-200-angular, ivf_pq index
-   \#1571 Meta engine type become IDMAP after dropping index for BINARY table
-   \#1574 Set all existing bitset in cache when applying deletes
-   \#1577 Row count incorrect if delete vectors then create index
-   \#1580 Old segment folder not removed after merge/compact if create_index is called before adding data
-   \#1590 Server down caused by failure to write file during concurrent mixed operations
-   \#1598 Server down during mixed operations
-   \#1601 External link bug in HTTP doc
-   \#1609 Refine Compact function
-   \#1808 Building index params check for Annoy
-   \#1852 Search index type<Annoy> failed with reason `failed to load index file`

## Feature
-   \#216 Add CLI to get server info
-   \#343 Add Opentracing
-   \#665 Support get/set config via CLI
-   \#759 Put C++ sdk out of milvus/core
-   \#766 If partition tag is similar, wrong partition is searched
-   \#771 Add server build commit info interface
-   \#788 Add web server into server module
-   \#813 Add push mode for prometheus monitor
-   \#815 Support MinIO storage
-   \#823 Support binary vector tanimoto/jaccard/hamming metric
-   \#830 Support WAL(write-ahead logging)
-   \#853 Support HNSW
-   \#861 Support DeleteById / SearchByID / GetVectorById / Flush
-   \#910 Change Milvus c++ standard to c++17
-   \#1122 Support AVX-512 in FAISS
-   \#1204 Add api to get table data information
-   \#1250 Support CPU profiling
-   \#1302 Get all record IDs in a segment by given a segment id
-   \#1461 Add crud APIs and segments APIs into http module
-   \#1463 Update config version to 0.2
-   \#1531 Remove S3 related config

## Improvement
-   \#738 Use Openblas / lapack from apt install
-   \#758 Enhance config description
-   \#791 Remove Arrow
-   \#834 Add cpu mode for built-in Faiss
-   \#848 Add ready-to-use config files to the Milvus repo for enhanced user experince
-   \#860 Remove redundant checks in CacheMgr's constructor
-   \#908 Move "primary_path" and "secondary_path" to storage config
-   \#931 Remove "collector" from config
-   \#966 Update NOTICE.md
-   \#1002 Rename minio to s3 in Storage Config section
-   \#1078 Move 'insert_buffer_size' to Cache Config section
-   \#1105 Error message is not clear when creating IVFSQ8H index without gpu resources
-   \#740, #849, #878, #972, #1033, #1161, #1173, #1199, #1190, #1223, #1222, #1257, #1264, #1269, #1164, #1303, #1304, #1324, #1388, #1459 Various fixes and improvements for Milvus documentation.
-   \#1297 Hide partition_name parameter, avid user directly access partition table
-   \#1234 Do S3 server validation check when Milvus startup
-   \#1263 Allow system conf modifiable and some take effect directly
-   \#1310 Add default partition tag for a table
-   \#1320 Remove debug logging from faiss
-   \#1426 Support to configure whether to enabled autoflush and the autoflush interval
-   \#1444 Improve delete
-   \#1448 General proto api for NNS libraries 
-   \#1480 Add return code for AVX512 selection
-   \#1524 Update config "preload_table" description
-   \#1544 Update resources name in HTTP module
-   \#1567 Update yaml config description

## Task
-   \#1327 Exclude third-party code from codebeat
-   \#1331 Exclude third-party code from codacy

# Milvus 0.6.0 (2019-12-07)

## Bug
-   \#228 Memory usage increased slowly during searching vectors
-   \#246 Exclude src/external folder from code coverage for jenkin ci
-   \#248 Reside src/external in thirdparty
-   \#316 Some files not merged after vectors added
-   \#327 Search does not use GPU when index type is FLAT
-   \#331 Add exception handle when search fail
-   \#340 Test cases run failed on 0.6.0
-   \#353 Rename config.h.in to version.h.in
-   \#374 sdk_simple return empty result
-   \#377 Create partition success if tag name only contains spaces
-   \#397 sdk_simple return incorrect result
-   \#399 Create partition should be failed if partition tag existed
-   \#412 Message returned is confused when partition created with null partition name
-   \#416 Drop the same partition success repeatally
-   \#440 Query API in customization still uses old version
-   \#440 Server cannot startup with gpu_resource_config.enable=false in GPU version
-   \#458 Index data is not compatible between 0.5 and 0.6
-   \#465 Server hang caused by searching with nsg index
-   \#485 Increase code coverage rate
-   \#486 gpu no usage during index building
-   \#497 CPU-version search performance decreased
-   \#504 The code coverage rate of core/src/scheduler/optimizer is too low
-   \#509 IVF_PQ index build trapped into dead loop caused by invalid params
-   \#513 Unittest DELETE_BY_RANGE sometimes failed
-   \#523 Erase file data from cache once the file is marked as deleted
-   \#527 faiss benchmark not compatible with faiss 1.6.0
-   \#530 BuildIndex stop when do build index and search simultaneously
-   \#532 Assigin value to `table_name` from confest shell
-   \#533 NSG build failed with MetricType Inner Product
-   \#543 client raise exception in shards when search results is empty
-   \#545 Avoid dead circle of build index thread when error occurs
-   \#547 NSG build failed using GPU-edition if set gpu_enable false
-   \#548 NSG search accuracy is too low
-   \#552 Server down during building index_type: IVF_PQ using GPU-edition
-   \#561 Milvus server should report exception/error message or terminate on mysql metadata backend error
-   \#579 Build index hang in GPU version when gpu_resources disabled
-   \#596 Frequently insert operation cost too much disk space
-   \#599 Build index log is incorrect
-   \#602 Optimizer specify wrong gpu_id
-   \#606 No log generated during building index with CPU
-   \#616 IP search metric_type is not supported by IVF_PQ index
-   \#631 FAISS isn't compiled with O3 option
-   \#636 (CPU) Create index PQ should be failed if table metric type set Inner Product
-   \#649 Typo "partiton" should be "partition"
-   \#654 Random crash when frequently insert vector one by one
-   \#658 Milvus error out when building SQ8H index without GPU resources
-   \#668 Update badge of README
-   \#670 Random failure of unittest db_test::SEARCH_TEST
-   \#674 Server down in stability test
-   \#696 Metric_type changed from IP to L2
-   \#705 Fix search SQ8H crash without GPU resource

## Feature
-   \#12 Pure CPU version for Milvus
-   \#77 Support table partition
-   \#127 Support new Index type IVFPQ
-   \#226 Experimental shards middleware for Milvus
-   \#227 Support new index types SPTAG-KDT and SPTAG-BKT
-   \#346 Support build index with multiple gpu
-   \#420 Update shards merge part to match v0.5.3
-   \#488 Add log in scheduler/optimizer
-   \#502 C++ SDK support IVFPQ and SPTAG
-   \#560 Add version in server config file
-   \#605 Print more messages when server start
-   \#644 Add a new rpc command to get milvus build version whether cpu or gpu
-   \#709 Show last commit id when server start

## Improvement
-   \#255 Add ivfsq8 test report detailed version
-   \#260 C++ SDK README
-   \#266 RPC request source code refactor
-   \#274 Logger the time cost during preloading data
-   \#275 Rename C++ SDK IndexType
-   \#284 Change C++ SDK to shared library
-   \#306 Use int64 for all config integer
-   \#310 Add Q&A for 'protocol https not supported or disable in libcurl' issue
-   \#314 add Find FAISS in CMake
-   \#322 Add option to enable / disable prometheus
-   \#354 Build migration scripts into milvus docker image
-   \#358 Add more information in build.sh and install.md
-   \#404 Add virtual method Init() in Pass abstract class
-   \#409 Add a Fallback pass in optimizer
-   \#433 C++ SDK query result is not easy to use
-   \#449 Add ShowPartitions example for C++ SDK
-   \#470 Small raw files should not be build index
-   \#584 Intergrate internal FAISS
-   \#611 Remove MILVUS_CPU_VERSION
-   \#634 FAISS GPU version is compiled with O0
-   \#737 Refactor server module to separate Grpc from server handler and scheduler

## Task

# Milvus 0.5.3 (2019-11-13)

## Bug
-   \#258 Bytes type in proto cause big-endian/little-endian problem

## Feature

## Improvement
-   \#204 improve grpc performance in search
-   \#207 Add more unittest for config set/get
-   \#208 Optimize unittest to support run single test more easily
-   \#284 Change C++ SDK to shared library
-   \#260 C++ SDK README

## Task

# Milvus 0.5.2 (2019-11-07)

## Bug
-   \#194 Search faild: message="Table file doesn't exist"

## Feature

## Improvement
-   \#190 Update default config:use_blas_threshold to 1100 and server version printout to 0.5.2

## Task

# Milvus 0.5.1 (2019-11-04)

## Bug
-   \#134 JFrog cache error
-   \#161 Search IVFSQHybrid crash on gpu
-   \#169 IVF_FLAT search out of memory

## Feature
-   \#90 The server start error messages could be improved to enhance user experience
-   \#104 test_scheduler core dump
-   \#115 Using new structure for tasktable
-   \#139 New config option use_gpu_threshold
-   \#146 Add only GPU and only CPU version for IVF_SQ8 and IVF_FLAT
-   \#164 Add CPU version for building index

## Improvement
-   \#64 Improvement dump function in scheduler
-   \#80 Print version information into log during server start
-   \#82 Move easyloggingpp into "external" directory
-   \#92 Speed up CMake build process
-   \#96 Remove .a file in milvus/lib for docker-version
-   \#118 Using shared_ptr instead of weak_ptr to avoid performance loss
-   \#122 Add unique id for Job
-   \#130 Set task state MOVED after resource copy it completed
-   \#149 Improve large query optimizer pass
-   \#156 Not return error when search_resources and index_build_device set cpu
-   \#159 Change the configuration name from 'use_gpu_threshold' to 'gpu_search_threshold'
-   \#168 Improve result reduce
-   \#175 add invalid config unittest

## Task

# Milvus 0.5.0 (2019-10-21)

## Bug
-   MS-568 Fix gpuresource free error
-   MS-572 Milvus crash when get SIGINT
-   MS-577 Unittest Query randomly hung
-   MS-587 Count get wrong result after adding vectors and index built immediately
-   MS-599 Search wrong result when table created with metric_type: IP
-   MS-601 Docker logs error caused by get CPUTemperature error
-   MS-605 Server going down during searching vectors
-   MS-620 Get table row counts display wrong error code
-   MS-622 Delete vectors should be failed if date range is invalid
-   MS-624 Search vectors failed if time ranges long enough
-   MS-637 Out of memory when load too many tasks
-   MS-639 SQ8H index created failed and server hang
-   MS-640 Cache object size calculate incorrect
-   MS-641 Segment fault(signal 11) in PickToLoad
-   MS-644 Search crashed with index-type: flat
-   MS-647 grafana display average cpu-temp
-   MS-652 IVFSQH quantization double free
-   MS-650 SQ8H index create issue
-   MS-653 When config check fail, Milvus close without message
-   MS-654 Describe index timeout when building index
-   MS-658 Fix SQ8 Hybrid can't search
-   MS-665 IVF_SQ8H search crash when no GPU resource in search_resources
-   \#9 Change default gpu_cache_capacity to 4
-   \#20 C++ sdk example get grpc error
-   \#23 Add unittest to improve code coverage
-   \#31 make clang-format failed after run build.sh -l
-   \#39 Create SQ8H index hang if using github server version
-   \#30 Some troubleshoot messages in Milvus do not provide enough information
-   \#48 Config unittest failed
-   \#59 Topk result is incorrect for small dataset

## Improvement
-   MS-552 Add and change the easylogging library
-   MS-553 Refine cache code
-   MS-555 Remove old scheduler
-   MS-556 Add Job Definition in Scheduler
-   MS-557 Merge Log.h
-   MS-558 Refine status code
-   MS-562 Add JobMgr and TaskCreator in Scheduler
-   MS-566 Refactor cmake
-   MS-574 Milvus configuration refactor
-   MS-578 Make sure milvus5.0 don't crack 0.3.1 data
-   MS-585 Update namespace in scheduler
-   MS-606 Speed up result reduce
-   MS-608 Update TODO names
-   MS-609 Update task construct function
-   MS-611 Add resources validity check in ResourceMgr
-   MS-619 Add optimizer class in scheduler
-   MS-626 Refactor DataObj to support cache any type data
-   MS-648 Improve unittest
-   MS-655 Upgrade SPTAG
-   \#42 Put union of index_build_device and search resources to gpu_pool
-   \#67 Avoid linking targets multiple times in cmake

## Feature
-   MS-614 Preload table at startup
-   MS-627 Integrate new index: IVFSQHybrid
-   MS-631 IVFSQ8H Index support
-   MS-636 Add optimizer in scheduler for FAISS_IVFSQ8H

## Task
-   MS-554 Change license to Apache 2.0
-   MS-561 Add contributing guidelines, code of conduct and README docs
-   MS-567 Add NOTICE.md
-   MS-569 Complete the NOTICE.md
-   MS-575 Add Clang-format & Clang-tidy & Cpplint
-   MS-586 Remove BUILD_FAISS_WITH_MKL option
-   MS-590 Refine cmake code to support cpplint
-   MS-600 Reconstruct unittest code
-   MS-602 Remove zilliz namespace
-   MS-610 Change error code base value from hex to decimal
-   MS-624 Re-organize project directory for open-source
-   MS-635 Add compile option to support customized faiss
-   MS-660 add ubuntu_build_deps.sh
-   \#18 Add all test cases

# Milvus 0.4.0 (2019-09-12)

## Bug
-   MS-119 The problem of combining the log files
-   MS-121 The problem that user can't change the time zone
-   MS-411 Fix metric unittest linking error
-   MS-412 Fix gpu cache logical error
-   MS-416 ExecutionEngineImpl::GpuCache has not return value cause crash
-   MS-417 YAML sequence load disable cause scheduler startup failed
-   MS-413 Create index failed and server exited
-   MS-427 Describe index error after drop index
-   MS-432 Search vectors params nprobe need to check max number
-   MS-431 Search vectors params nprobe: 0/-1, expected result: raise exception
-   MS-331 Crate Table : when table exists, error code is META_FAILED(code=15) rather than ILLEGAL TABLE NAME(code=9))
-   MS-430 Search no result if index created with FLAT
-   MS-443 Create index hang again
-   MS-436 Delete vectors failed if index created with index_type: IVF_FLAT/IVF_SQ8
-   MS-449 Add vectors twice success, once with ids, the other no ids
-   MS-450 server hang after run stop_server.sh
-   MS-458 Keep building index for one file when no gpu resource
-   MS-461 Mysql meta unittest failed
-   MS-462 Run milvus server twices, should display error
-   MS-463 Search timeout
-   MS-467 mysql db test failed
-   MS-470 Drop index success, which table not created
-   MS-471 code coverage run failed
-   MS-492 Drop index failed if index have been created with index_type: FLAT
-   MS-493 Knowhere unittest crash
-   MS-453 GPU search error when nprobe set more than 1024
-   MS-474 Create index hang if use branch-0.3.1 server config
-   MS-510 unittest out of memory and crashed
-   MS-507 Dataset 10m-512, index type sq8，performance in-normal when set CPU_CACHE to 16 or 64
-   MS-543 SearchTask fail without exception
-   MS-582 grafana displays changes frequently

## Improvement
-   MS-327 Clean code for milvus
-   MS-336 Scheduler interface
-   MS-344 Add TaskTable Test
-   MS-345 Add Node Test
-   MS-346 Add some implementation of scheduler to solve compile error
-   MS-348 Add ResourceFactory Test
-   MS-350 Remove knowhere submodule
-   MS-354 Add task class and interface in scheduler
-   MS-355 Add copy interface in ExcutionEngine
-   MS-357 Add minimum schedule function
-   MS-359 Add cost test in new scheduler
-   MS-361 Add event in resource
-   MS-364 Modify tasktableitem in tasktable
-   MS-365 Use tasktableitemptr instead in event
-   MS-366 Implement TaskTable
-   MS-368 Implement cost.cpp
-   MS-371 Add TaskTableUpdatedEvent
-   MS-373 Add resource test
-   MS-374 Add action definition
-   MS-375 Add Dump implementation for Event
-   MS-376 Add loader and executor enable flag in Resource avoid diskresource execute task
-   MS-377 Improve process thread trigger in ResourceMgr, Scheduler and TaskTable
-   MS-378 Debug and Update normal_test in scheduler unittest
-   MS-379 Add Dump implementation in Resource
-   MS-380 Update resource loader and executor, work util all finished
-   MS-383 Modify condition variable usage in scheduler
-   MS-384 Add global instance of ResourceMgr and Scheduler
-   MS-389 Add clone interface in Task
-   MS-390 Update resource construct function
-   MS-391 Add PushTaskToNeighbourHasExecutor action
-   MS-394 Update scheduler unittest
-   MS-400 Add timestamp record in task state change function
-   MS-402 Add dump implementation for TaskTableItem
-   MS-406 Add table flag for meta
-   MS-403 Add GpuCacheMgr
-   MS-404 Release index after search task done avoid memory increment continues
-   MS-405 Add delete task support
-   MS-407 Reconstruct MetricsCollector
-   MS-408 Add device_id in resource construct function
-   MS-409 Using new scheduler
-   MS-413 Remove thrift dependency
-   MS-410 Add resource config comment
-   MS-414 Add TaskType in Scheduler::Task
-   MS-415 Add command tasktable to dump all tasktables
-   MS-418 Update server_config.template file, set CPU compute only default
-   MS-419 Move index_file_size from IndexParam to TableSchema
-   MS-421 Add TaskLabel in scheduler
-   MS-422 Support DeleteTask in Multi-GpuResource case
-   MS-428 Add PushTaskByDataLocality in scheduler
-   MS-440 Add DumpTaskTables in sdk
-   MS-442 Merge Knowhere
-   MS-445 Rename CopyCompleted to LoadCompleted
-   MS-451 Update server_config.template file, set GPU compute default
-   MS-455 Distribute tasks by minimal cost in scheduler
-   MS-460 Put transport speed as weight when choosing neighbour to execute task
-   MS-459 Add cache for pick function in tasktable
-   MS-476 Improve search performance
-   MS-482 Change search stream transport to unary in grpc
-   MS-487 Define metric type in CreateTable
-   MS-488 Improve code format in scheduler
-   MS-495 cmake: integrated knowhere
-   MS-496 Change the top_k limitation from 1024 to 2048
-   MS-502 Update tasktable_test in scheduler
-   MS-504 Update node_test in scheduler
-   MS-505 Install core unit test and add to coverage
-   MS-508 Update normal_test in scheduler
-   MS-532 Add grpc server unittest
-   MS-511 Update resource_test in scheduler
-   MS-517 Update resource_mgr_test in scheduler
-   MS-518 Add schedinst_test in scheduler
-   MS-519 Add event_test in scheduler
-   MS-520 Update resource_test in scheduler
-   MS-524 Add some unittest in event_test and resource_test
-   MS-525 Disable parallel reduce in SearchTask
-   MS-527 Update scheduler_test and enable it
-   MS-528 Hide some config used future
-   MS-530 Add unittest for SearchTask->Load
-   MS-531 Disable next version code
-   MS-533 Update resource_test to cover dump function
-   MS-523 Config file validation
-   MS-539 Remove old task code
-   MS-546 Add simple mode resource_config
-   MS-570 Add prometheus docker-compose file
-   MS-576 Scheduler refactor
-   MS-592 Change showtables stream transport to unary

## Feature
-   MS-343 Implement ResourceMgr
-   MS-338 NewAPI: refine code to support CreateIndex
-   MS-339 NewAPI: refine code to support DropIndex
-   MS-340 NewAPI: implement DescribeIndex

## Task
-   MS-297 disable mysql unit test

# Milvus 0.3.1 (2019-07-10)

## Bug

-   MS-148 Disable cleanup if mode is read only
-   MS-149 Fixed searching only one index file issue in distributed mode
-   MS-153 Fix c_str error when connecting to MySQL
-   MS-157 Fix changelog
-   MS-190 Use env variable to switch mem manager and fix cmake
-   MS-217 Fix SQ8 row count bug
-   MS-224 Return AlreadyExist status in MySQLMetaImpl::CreateTable if table already exists
-   MS-232 Add MySQLMetaImpl::UpdateTableFilesToIndex and set maximum_memory to default if config value = 0
-   MS-233 Remove mem manager log
-   MS-230 Change parameter name: Maximum_memory to insert_buffer_size
-   MS-234 Some case cause background merge thread stop
-   MS-235 Some test cases random fail
-   MS-236 Add MySQLMetaImpl::HasNonIndexFiles
-   MS-257 Update bzip2 download url
-   MS-288 Update compile scripts
-   MS-330 Stability test failed caused by server core dumped
-   MS-347 Build index hangs again
-   MS-382 fix MySQLMetaImpl::CleanUpFilesWithTTL unknown column bug

## Improvement
-   MS-156 Add unittest for merge result functions
-   MS-152 Delete assert in MySQLMetaImpl and change MySQLConnectionPool impl
-   MS-204 Support multi db_path
-   MS-206 Support SQ8 index type
-   MS-208 Add buildinde interface for C++ SDK
-   MS-212 Support Inner product metric type
-   MS-241 Build Faiss with MKL if using Intel CPU; else build with OpenBlas
-   MS-242 Clean up cmake and change MAKE_BUILD_ARGS to be user defined variable
-   MS-245 Improve search result transfer performance
-   MS-248 Support AddVector/SearchVector profiling
-   MS-256 Add more cache config
-   MS-260 Refine log
-   MS-249 Check machine hardware during initialize
-   MS-261 Update faiss version to 1.5.3 and add BUILD_FAISS_WITH_MKL as an option
-   MS-266 Improve topk reduce time by using multi-threads
-   MS-275 Avoid sqlite logic error excetion
-   MS-278 add IndexStatsHelper
-   MS-313 add GRPC
-   MS-325 add grpc status return for C++ sdk and modify some format
-   MS-278 Add IndexStatsHelper
-   MS-312 Set openmp thread number by config
-   MS-305 Add CPU core percent metric
-   MS-310 Add milvus CPU utilization ratio and CPU/GPU temperature metrics
-   MS-324 Show error when there is not enough gpu memory to build index
-   MS-328 Check metric type on server start
-   MS-332 Set grpc and thrift server run concurrently
-   MS-352 Add hybrid index

## Feature
-   MS-180 Add new mem manager
-   MS-195 Add nlist and use_blas_threshold conf
-   MS-137 Integrate knowhere

## Task

-   MS-125 Create 0.3.1 release branch
-   MS-306 Optimize build efficiency

# Milvus 0.3.0 (2019-06-30)

## Bug
-   MS-104 Fix unittest lcov execution error
-   MS-102 Fix build script file condition error
-   MS-80 Fix server hang issue
-   MS-89 Fix compile failed, libgpufaiss.a link missing
-   MS-90 Fix arch match incorrect on ARM
-   MS-99 Fix compilation bug
-   MS-110 Avoid huge file size

## Improvement
-   MS-82 Update server startup welcome message
-   MS-83 Update vecwise to Milvus
-   MS-77 Performance issue of post-search action
-   MS-22 Enhancement for MemVector size control
-   MS-92 Unify behavior of debug and release build
-   MS-98 Install all unit test to installation directory
-   MS-115 Change is_startup of metric_config switch from true to on
-   MS-122 Archive criteria config
-   MS-124 HasTable interface
-   MS-126 Add more error code
-   MS-128 Change default db path

## Feature

-   MS-57 Implement index load/search pipeline
-   MS-56 Add version information when server is started
-   MS-64 Different table can have different index type
-   MS-52 Return search score
-   MS-66 Support time range query
-   MS-68 Remove rocksdb from third-party
-   MS-70 cmake: remove redundant libs in src
-   MS-71 cmake: fix faiss dependency
-   MS-72 cmake: change prometheus source to git
-   MS-73 cmake: delete civetweb
-   MS-65 Implement GetTableRowCount interface
-   MS-45 Implement DeleteTable interface
-   MS-75 cmake: change faiss version to 1.5.2; add CUDA gencode
-   MS-81 Fix faiss ptx issue; change cuda gencode
-   MS-84 cmake: add arrow, jemalloc and jsoncons third party; default build option OFF
-   MS-85 add NetIO metric
-   MS-96 add new query interface for specified files
-   MS-97 Add S3 SDK for MinIO Storage
-   MS-105 Add MySQL
-   MS-130 Add prometheus_test
-   MS-144 Add nprobe config
-   MS-147 Enable IVF
-   MS-130 Add prometheus_test

## Task
-   MS-74 Change README.md in cpp
-   MS-88 Add support for arm architecture

# Milvus 0.2.0 (2019-05-31)

## Bug

-   MS-32 Fix thrift error
-   MS-34 Fix prometheus-cpp thirdparty
-   MS-67 Fix license check bug
-   MS-76 Fix pipeline crash bug
-   MS-100 CMake: fix AWS build issue
-   MS-101 Change AWS build type to Release

## Improvement

-   MS-20 Clean Code Part 1

## Feature

-   MS-5 Implement Auto Archive Feature
-   MS-6 Implement SDK interface part 1
-   MS-16 Implement metrics without prometheus
-   MS-21 Implement SDK interface part 2
-   MS-26 CMake. Add thirdparty packages
-   MS-31 CMake: add prometheus
-   MS-33 CMake: add -j4 to make third party packages build faster
-   MS-27 Support gpu config and disable license build config in cmake
-   MS-47 Add query vps metrics
-   MS-37 Add query, cache usage, disk write speed and file data size metrics
-   MS-30 Use faiss v1.5.2
-   MS-54 CMake: Change Thrift third party URL to github.com
-   MS-69 Prometheus: add all proposed metrics

## Task

-   MS-1 Add CHANGELOG.md
-   MS-4 Refactor the vecwise_engine code structure
-   MS-62 Search range to all if no date specified
