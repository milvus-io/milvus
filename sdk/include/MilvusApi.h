// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#include "BooleanQuery.h"
#include "Field.h"
#include "Status.h"

/** \brief Milvus SDK namespace
 */
namespace milvus {

/**
 * @brief Index Type
 */
enum class IndexType {
    INVALID = 0,
    FLAT = 1,
    IVFFLAT = 2,
    IVFSQ8 = 3,
    RNSG = 4,
    IVFSQ8H = 5,
    IVFPQ = 6,
    SPTAGKDT = 7,
    SPTAGBKT = 8,
    HNSW = 11,
    ANNOY = 12,
};

enum class MetricType {
    L2 = 1,        // Euclidean Distance
    IP = 2,        // Cosine Similarity
    HAMMING = 3,   // Hamming Distance
    JACCARD = 4,   // Jaccard Distance
    TANIMOTO = 5,  // Tanimoto Distance
    SUBSTRUCTURE = 6,   // Substructure Distance
    SUPERSTRUCTURE = 7,  // Superstructure Distance
};

/**
 * @brief Connect API parameter
 */
struct ConnectParam {
    std::string ip_address;  ///< Server IP address
    std::string port;        ///< Server PORT
};

/**
 * @brief Collection parameters
 */
struct CollectionParam {
    std::string collection_name;              ///< Collection_name name
    int64_t dimension = 0;                    ///< Vector dimension, must be a positive value
    int64_t index_file_size = 1024;           ///< Index file size, must be a positive value, unit: MB
    MetricType metric_type = MetricType::L2;  ///< Index metric type
};

/**
 * @brief TopK query result
 */
struct QueryResult {
    std::vector<int64_t> ids;      ///< Query entity ids result
    std::vector<float> distances;  ///< Query distances result
};
using TopKQueryResult = std::vector<QueryResult>;  ///< Topk query result

/**
 * @brief Index parameters
 * Note: extra_params is extra parameters list, it must be json format
 *       For different index type, parameter list is different accordingly, for example:
 *       FLAT/IVFLAT/SQ8:  {nlist: 16384}
 *           ///< nlist range:[1, 999999]
 *       IVFPQ:  {nlist: 16384, m: 12}
 *           ///< nlist range:[1, 999999]
 *           ///< m is decided by dim and have a couple of results.
 *       NSG:  {search_length: 45, out_degree:50, candidate_pool_size:300, knng:100}
 *           ///< search_length range:[10, 300]
 *           ///< out_degree range:[5, 300]
 *           ///< candidate_pool_size range:[50, 1000]
 *           ///< knng range:[5, 300]
 *       HNSW  {M: 16, efConstruction:300}
 *           ///< M range:[5, 48]
 *           ///< efConstruction range:[100, 500]
 */
struct IndexParam {
    std::string collection_name;        ///< Collection name for create index
    IndexType index_type;               ///< Index type
    std::string extra_params;           ///< Extra parameters according to different index type, must be json format
};

/**
 * @brief partition parameters
 */
struct PartitionParam {
    std::string collection_name;
    std::string partition_tag;
};

using PartitionTagList = std::vector<std::string>;

/**
 * @brief segment statistics
 */
struct SegmentStat {
    std::string segment_name;    ///< Segment name
    int64_t row_count;           ///< Segment row count
    std::string index_name;      ///< Segment index name
    int64_t data_size;           ///< Segment data size
};

/**
 * @brief partition statistics
 */
struct PartitionStat {
    std::string tag;                          ///< Partition tag
    int64_t row_count;                        ///< Partition row count
    std::vector<SegmentStat> segments_stat;   ///< Partition's segments statistics
};

/**
 * @brief collection info
 */
struct CollectionInfo {
    int64_t total_row_count;                      ///< Collection total entity count
    std::vector<PartitionStat> partitions_stat;   ///< Collection's partitions statistics
};



struct HMapping {
    std::string collection_name;
    std::vector<FieldPtr> numerica_fields;
    std::vector<VectorFieldPtr> vector_fields;
};

struct HEntity {
    std::unordered_map<std::string, std::vector<std::string>> numerica_value;
    std::unordered_map<std::string, std::vector<Entity>> vector_value;
};




/**
 * @brief SDK main class
 */
class Connection {
 public:
    /**
     * @brief Create connection
     *
     * Create a connection instance and return it's shared pointer
     *
     * @return connection instance pointer
     */

    static std::shared_ptr<Connection>
    Create();

    /**
     * @brief Destroy connection
     *
     * Destroy the connection instance
     *
     * @param connection, the shared pointer to the instance to be destroyed
     *
     * @return if destroy is successful
     */

    static Status
    Destroy(std::shared_ptr<Connection>& connection_ptr);

    /**
     * @brief Connect
     *
     * This method is used to connect server.
     * Connect function should be called before any operations.
     *
     * @param param, use to provide server information
     *
     * @return Indicate if connect is successful
     */

    virtual Status
    Connect(const ConnectParam& connect_param) = 0;

    /**
     * @brief Connect
     *
     * This method is used to connect server.
     * Connect function should be called before any operations.
     *
     * @param uri, use to provide server uri, example: milvus://ipaddress:port
     *
     * @return Indicate if connect is successful
     */
    virtual Status
    Connect(const std::string& uri) = 0;

    /**
     * @brief Connected
     *
     * This method is used to test whether server is connected.
     *
     * @return Indicate if connection status
     */
    virtual Status
    Connected() const = 0;

    /**
     * @brief Disconnect
     *
     * This method is used to disconnect server.
     *
     * @return Indicate if disconnect is successful
     */
    virtual Status
    Disconnect() = 0;

    /**
     * @brief Get the client version
     *
     * This method is used to give the client version.
     *
     * @return Client version.
     */
    virtual std::string
    ClientVersion() const = 0;

    /**
     * @brief Get the server version
     *
     * This method is used to give the server version.
     *
     * @return Server version.
     */
    virtual std::string
    ServerVersion() const = 0;

    /**
     * @brief Get the server status
     *
     * This method is used to give the server status.
     *
     * @return Server status.
     */
    virtual std::string
    ServerStatus() const = 0;

    /**
     * @brief Get config method
     *
     * This method is used to set config.
     *
     * @param node_name, config node name.
     * @param value, config value.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    GetConfig(const std::string& node_name, std::string& value) const = 0;

    /**
     * @brief Set config method
     *
     * This method is used to set config.
     *
     * @param node_name, config node name.
     * @param value, config value.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    SetConfig(const std::string& node_name, const std::string& value) const = 0;

    /**
     * @brief Create collection method
     *
     * This method is used to create collection.
     *
     * @param param, use to provide collection information to be created.
     *
     * @return Indicate if collection is created successfully
     */
    virtual Status
    CreateCollection(const CollectionParam& param) = 0;

    /**
     * @brief Test collection existence method
     *
     * This method is used to test collection existence.
     *
     * @param collection_name, target collection's name.
     *
     * @return Indicate if collection is cexist
     */
    virtual bool
    HasCollection(const std::string& collection_name) = 0;

    /**
     * @brief Drop collection method
     *
     * This method is used to drop collection(and its partitions).
     *
     * @param collection_name, target collection's name.
     *
     * @return Indicate if collection is drop successfully.
     */
    virtual Status
    DropCollection(const std::string& collection_name) = 0;

    /**
     * @brief Create index method
     *
     * This method is used to create index for whole collection(and its partitions).
     *
     * @param index_param, use to provide index information to be created.
     *
     * @return Indicate if create index successfully.
     */
    virtual Status
    CreateIndex(const IndexParam& index_param) = 0;

    /**
     * @brief Insert entity to collection
     *
     * This method is used to insert vector array to collection.
     *
     * @param collection_name, target collection's name.
     * @param partition_tag, target partition's tag, keep empty if no partition specified.
     * @param entity_array, entity array is inserted, each entitu represent a vector.
     * @param id_array,
     *  specify id for each entity,
     *  if this array is empty, milvus will generate unique id for each entity,
     *  and return all ids by this parameter.
     *
     * @return Indicate if entity array are inserted successfully
     */
    virtual Status
    Insert(const std::string& collection_name,
           const std::string& partition_tag,
           const std::vector<Entity>& entity_array,
           std::vector<int64_t>& id_array) = 0;

    /**
     * @brief Get entity data by id
     *
     * This method is used to get entity data by id from a collection.
     * Return the first found entity if there are entities with duplicated id
     *
     * @param collection_name, target collection's name.
     * @param entity_id, target entity id.
     * @param entity_data, returned entity data.
     *
     * @return Indicate if the operation is succeed.
     */
    virtual Status
    GetEntityByID(const std::string& collection_name, int64_t entity_id, Entity& entity_data) = 0;

    /**
     * @brief Get entity ids from a segment
     *
     * This method is used to get entity ids from a segment
     * Return all entity(not deleted) ids
     *
     * @param collection_name, target collection's name.
     * @param segment_name, target segment name.
     * @param id_array, returned entity id array.
     *
     * @return Indicate if the operation is succeed.
     */
    virtual Status
    GetIDsInSegment(const std::string& collection_name,
                    const std::string& segment_name,
                    std::vector<int64_t>& id_array) = 0;

    /**
     * @brief Search entities in a collection
     *
     * This method is used to query entity in collection.
     *
     * @param collection_name, target collection's name.
     * @param partition_tag_array, target partitions, keep empty if no partition specified.
     * @param query_entity_array, vectors to be queried.
     * @param topk, how many similarity entities will be returned.
     * @param extra_params, extra search parameters according to different index type, must be json format.
     * Note: extra_params is extra parameters list, it must be json format, for example:
     *       For different index type, parameter list is different accordingly
     *       FLAT/IVFLAT/SQ8/IVFPQ:  {nprobe: 32}
     *           ///< nprobe range:[1,999999]
     *       NSG:  {search_length:100}
     *           ///< search_length range:[10, 300]
     *       HNSW  {ef: 64}
     *           ///< ef range:[topk, 4096]
     * @param topk_query_result, result array.
     *
     * @return Indicate if query is successful.
     */
    virtual Status
    Search(const std::string& collection_name, const PartitionTagList& partition_tag_array,
           const std::vector<Entity>& entity_array, int64_t topk,
           const std::string& extra_params, TopKQueryResult& topk_query_result) = 0;

    /**
     * @brief Show collection description
     *
     * This method is used to show collection information.
     *
     * @param collection_name, target collection's name.
     * @param collection_param, collection_param is given when operation is successful.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    DescribeCollection(const std::string& collection_name, CollectionParam& collection_param) = 0;

    /**
     * @brief Get collection entity count
     *
     * This method is used to get collection entity count.
     *
     * @param collection_name, target collection's name.
     * @param entity_count, collection total entity count(including partitions).
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    CountCollection(const std::string& collection_name, int64_t& entity_count) = 0;

    /**
     * @brief Show all collections in database
     *
     * This method is used to list all collections.
     *
     * @param collection_array, all collections in database.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    ShowCollections(std::vector<std::string>& collection_array) = 0;

    /**
     * @brief Show collection information
     *
     * This method is used to get detail information of a collection.
     *
     * @param collection_name, target collection's name.
     * @param collection_info, target collection's information
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    ShowCollectionInfo(const std::string& collection_name, CollectionInfo& collection_info) = 0;

    /**
     * @brief Delete entity by id
     *
     * This method is used to delete entity by id.
     *
     * @param collection_name, target collection's name.
     * @param id_array, entity id array to be deleted.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    DeleteByID(const std::string& collection_name, const std::vector<int64_t>& id_array) = 0;

    /**
     * @brief Preload collection
     *
     * This method is used to preload collection data into memory
     *
     * @param collection_name, target collection's name.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    PreloadCollection(const std::string& collection_name) const = 0;

    /**
     * @brief Describe index
     *
     * This method is used to describe index
     *
     * @param collection_name, target collection's name.
     * @param index_param, returned index information.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    DescribeIndex(const std::string& collection_name, IndexParam& index_param) const = 0;

    /**
     * @brief Drop index
     *
     * This method is used to drop index of collection(and its partitions)
     *
     * @param collection_name, target collection's name.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    DropIndex(const std::string& collection_name) const = 0;

    /**
     * @brief Create partition method
     *
     * This method is used to create collection's partition
     *
     * @param partition_param, use to provide partition information to be created.
     *
     * @return Indicate if partition is created successfully
     */
    virtual Status
    CreatePartition(const PartitionParam& partition_param) = 0;

    /**
     * @brief Show all partitions method
     *
     * This method is used to show all partitions(return their tags)
     *
     * @param collection_name, target collection's name.
     * @param partition_tag_array, partition tag array of the collection.
     *
     * @return Indicate if this operation is successful
     */
    virtual Status
    ShowPartitions(const std::string& collection_name, PartitionTagList& partition_tag_array) const = 0;

    /**
     * @brief Delete partition method
     *
     * This method is used to delete collection's partition.
     *
     * @param partition_param, target partition to be deleted.
     *
     * @return Indicate if partition is delete successfully.
     */
    virtual Status
    DropPartition(const PartitionParam& partition_param) = 0;

    /**
     * @brief Flush collection buffer into storage
     *
     * This method is used to flush collection buffer into storage
     *
     * @param collection_name, target collection's name.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    FlushCollection(const std::string& collection_name) = 0;

    /**
     * @brief Flush all buffer into storage
     *
     * This method is used to all collection buffer into storage
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    Flush() = 0;

    /**
     * @brief Compact collection, permanently remove deleted vectors
     *
     * This method is used to compact collection
     *
     * @param collection_name, target collection's name.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    CompactCollection(const std::string& collection_name) = 0;

    /*******************************New Interface**********************************/

    virtual Status
    CreateHybridCollection(const HMapping& mapping) = 0;

    virtual Status
    InsertEntity(const std::string& collection_name,
                 const std::string& partition_tag,
                 HEntity& entities,
                 std::vector<uint64_t>& id_array) = 0;

    virtual Status
    HybridSearch(const std::string& collection_name,
                 const std::vector<std::string>& partition_list,
                 BooleanQueryPtr& boolean_query,
                 const std::string& extra_params,
                 TopKQueryResult& topk_query_result) = 0;
};

}  // namespace milvus
