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

#include <any>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

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
    RHNSWFLAT = 13,
    RHNSWPQ = 14,
    RHNSWSQ = 15,
    NGTPANNG = 16,
    NGTONNG = 17,
};

enum class MetricType {
    L2 = 1,              // Euclidean Distance
    IP = 2,              // Cosine Similarity
    HAMMING = 3,         // Hamming Distance
    JACCARD = 4,         // Jaccard Distance
    TANIMOTO = 5,        // Tanimoto Distance
    SUBSTRUCTURE = 6,    // Substructure Distance
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
 * @brief Attribute record
 */
struct AttrRecord {
    std::vector<int64_t> int_record;
    std::vector<double> double_record;
};

/**
 * @brief field value
 */
struct FieldValue {
    int64_t row_num;
    std::unordered_map<std::string, std::vector<int8_t>> int8_value;
    std::unordered_map<std::string, std::vector<int16_t>> int16_value;
    std::unordered_map<std::string, std::vector<int32_t>> int32_value;
    std::unordered_map<std::string, std::vector<int64_t>> int64_value;
    std::unordered_map<std::string, std::vector<float>> float_value;
    std::unordered_map<std::string, std::vector<double>> double_value;
    std::unordered_map<std::string, std::vector<VectorData>> vector_value;
};

/**
 * @brief Vector parameters
 */
struct VectorParam {
    std::string json_param;
    std::vector<VectorData> vector_records;
};

/**
 * @brief query result
 */
struct QueryResult {
    std::vector<int64_t> ids;      ///< Query entity ids result
    std::vector<float> distances;  ///< Query distances result
    FieldValue field_value;
};
using TopKQueryResult = std::vector<QueryResult>;  ///< Topk hybrid query result

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
    std::string collection_name;  ///< Collection name for create index
    std::string field_name;       ///< Field name
    std::string index_params;     ///< Extra parameters according to different index type, must be json format
};

/**
 * @brief partition parameters
 */
struct PartitionParam {
    std::string collection_name;
    std::string partition_tag;
};

using PartitionTagList = std::vector<std::string>;

struct Mapping {
    std::string collection_name;
    std::vector<FieldPtr> fields;
    std::string extra_params;
};

/**
 * @brief SDK main class
 */
class Connection {
 public:
    /**
     * @brief Create connection instance
     *
     * Create a connection instance and return its shared pointer
     *
     * @return connection instance pointer
     */

    static std::shared_ptr<Connection>
    Create();

    /**
     * @brief Destroy connection instance
     *
     * Destroy the connection instance
     *
     * @param connection, the shared pointer to the instance to be destroyed
     *
     * @return Indicate if destroy successfully
     */

    static Status
    Destroy(std::shared_ptr<Connection>& connection_ptr);

    /**
     * @brief Connect
     *
     * This method is used to connect to Milvus server.
     * Connect function must be called before all other operations.
     *
     * @param param, used to provide server information
     *
     * @return Indicate if connect successfully
     */

    virtual Status
    Connect(const ConnectParam& connect_param) = 0;

    /**
     * @brief Connect
     *
     * This method is used to connect to Milvus server.
     * Connect function must be called before all other operations.
     *
     * @param uri, used to provide server uri, example: milvus://ipaddress:port
     *
     * @return Indicate if connect successfully
     */
    virtual Status
    Connect(const std::string& uri) = 0;

    /**
     * @brief Check connection
     *
     * This method is used to check whether Milvus server is connected.
     *
     * @return Indicate if connection status
     */
    virtual Status
    Connected() const = 0;

    /**
     * @brief Disconnect
     *
     * This method is used to disconnect from Milvus server.
     *
     * @return Indicate if disconnect successfully
     */
    virtual Status
    Disconnect() = 0;

    /**
     * @brief Create collection method
     *
     * This method is used to create collection.
     *
     * @param param, used to provide collection information to be created.
     *
     * @return Indicate if collection is created successfully
     */
    virtual Status
    CreateCollection(const Mapping& mapping, const std::string& extra_params) = 0;

    /**
     * @brief Drop collection method
     *
     * This method is used to drop collection (and its partitions).
     *
     * @param collection_name, target collection's name.
     *
     * @return Indicate if collection is dropped successfully.
     */
    virtual Status
    DropCollection(const std::string& collection_name) = 0;

    /**
     * @brief Test collection existence method
     *
     * This method is used to test collection existence.
     *
     * @param collection_name, target collection's name.
     *
     * @return Indicate if the collection exists
     */
    virtual bool
    HasCollection(const std::string& collection_name) = 0;

    /**
     * @brief List all collections in database
     *
     * This method is used to list all collections.
     *
     * @param collection_array, all collections in database.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    ListCollections(std::vector<std::string>& collection_array) = 0;

    /**
     * @brief Get collection information
     *
     * This method is used to get collection information.
     *
     * @param collection_name, target collection's name.
     * @param collection_param, collection_param is given when operation is successful.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    GetCollectionInfo(const std::string& collection_name, Mapping& mapping) = 0;

    /**
     * @brief Get collection statistics
     *
     * This method is used to get statistics of a collection.
     *
     * @param collection_name, target collection's name.
     * @param collection_stats, target collection's statistics in json format
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    GetCollectionStats(const std::string& collection_name, std::string& collection_stats) = 0;

    /**
     * @brief Get collection entity count
     *
     * This method is used to get collection entity count.
     *
     * @param collection_name, target collection's name.
     * @param entity_count, total entity count in collection.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    CountEntities(const std::string& collection_name, int64_t& entity_count) = 0;

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
     * @brief Has partition method
     *
     * This method is used to test existence of collection's partition
     *
     * @param collection_name, target collection's name.
     * @param partition_tag, target partition's tag.
     *
     * @return Indicate if partition is created successfully
     */
    virtual bool
    HasPartition(const std::string& collection_name, const std::string& partition_tag) const = 0;

    /**
     * @brief List all partitions method
     *
     * This method is used to list all partitions(return their tags)
     *
     * @param collection_name, target collection's name.
     * @param partition_tag_array, partition tag array of the collection.
     *
     * @return Indicate if this operation is successful
     */
    virtual Status
    ListPartitions(const std::string& collection_name, PartitionTagList& partition_tag_array) const = 0;

    /**
     * @brief Create index method
     *
     * This method is used to create index for collection.
     *
     * @param collection_name, target collection's name.
     * @param field_name, target field name.
     * @param index_name, name of index.
     * @param index_params, extra informations of index such as index type, must be json format.
     *
     * @return Indicate if create index successfully.
     */
    virtual Status
    CreateIndex(const IndexParam& index_param) = 0;

    /**
     * @brief Drop index method
     *
     * This method is used to drop index of collection.
     *
     * @param collection_name, target collection's name.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    DropIndex(const std::string& collection_name, const std::string& field_name,
              const std::string& index_name) const = 0;

    /**
     * @brief Insert entity to collection
     *
     * This method is used to insert vector array to collection.
     *
     * @param collection_name, target collection's name.
     * @param partition_tag, target partition's tag, keep empty if no partition specified.
     * @param entity_array, entity array is inserted, each entity represent a vector.
     * @param id_array,
     *  specify id for each entity,
     *  if this array is empty, milvus will generate unique id for each entity,
     *  and return all ids by this parameter.
     *
     * @return Indicate if entity array are inserted successfully
     */
    virtual Status
    Insert(const std::string& collection_name, const std::string& partition_tag, const FieldValue& entity_array,
           std::vector<int64_t>& id_array) = 0;

    /**
     * @brief Get entity data by id
     *
     * This method is used to get entities data by id array from a collection.
     * Return the first found entity if there are entities with duplicated id
     *
     * @param collection_name, target collection's name.
     * @param id_array, target entities id array.
     * @param entities_data, returned entities data.
     *
     * @return Indicate if the operation is succeed.
     */
    virtual Status
    GetEntityByID(const std::string& collection_name, const std::vector<int64_t>& id_array, std::string& entities) = 0;

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
    DeleteEntityByID(const std::string& collection_name, const std::vector<int64_t>& id_array) = 0;

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
    Search(const std::string& collection_name, const std::vector<std::string>& partition_list, const std::string& dsl,
           const VectorParam& vector_param, TopKQueryResult& query_result) = 0;

    virtual Status
    SearchPB(const std::string& collection_name, const std::vector<std::string>& partition_list,
             BooleanQueryPtr& boolean_query, const std::string& extra_params, TopKQueryResult& query_result) = 0;

    /**
     * @brief List entity ids from a segment
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
    ListIDInSegment(const std::string& collection_name, const int64_t& segment_id,
                    std::vector<int64_t>& id_array) = 0;

    /**
     * @brief Load collection into memory
     *
     * This method is used to load collection data into memory
     *
     * @param collection_name, target collection's name.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    LoadCollection(const std::string& collection_name) const = 0;

    /**
     * @brief Flush collections insert buffer into storage
     *
     * This method is used to flush collection insert buffer into storage
     *
     * @param collection_name_array, target collections name array.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    Flush(const std::vector<std::string>& collection_name_array) = 0;

    /**
     * @brief Compact collection, permanently remove deleted vectors
     *
     * This method is used to compact collection
     *
     * @param collection_name, target collection's name.
     * @param threshold
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status
    Compact(const std::string& collection_name, const double& threshold) = 0;
};

}  // namespace milvus
