#!/usr/local/bin/thrift --java --php --py
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# *** PLEASE REMEMBER TO EDIT THE VERSION CONSTANT WHEN MAKING CHANGES ***
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#
# Interface definition for Cassandra Service
#

namespace netcore Apache.Cassandra.Test

# Thrift.rb has a bug where top-level modules that include modules 
# with the same name are not properly referenced, so we can't do
# Cassandra::Cassandra::Client.
namespace rb CassandraThrift

# The API version (NOT the product version), composed as a dot delimited
# string with major, minor, and patch level components.
#
#  - Major: Incremented for backward incompatible changes. An example would
#           be changes to the number or disposition of method arguments.
#  - Minor: Incremented for backward compatible changes. An example would
#           be the addition of a new (optional) method.
#  - Patch: Incremented for bug fixes. The patch level should be increased
#           for every edit that doesn't result in a change to major/minor.
#
# See the Semantic Versioning Specification (SemVer) http://semver.org.
const string VERSION = "19.24.0"


#
# data structures
#

/** Basic unit of data within a ColumnFamily.
 * @param name, the name by which this column is set and retrieved.  Maximum 64KB long.
 * @param value. The data associated with the name.  Maximum 2GB long, but in practice you should limit it to small numbers of MB (since Thrift must read the full value into memory to operate on it).
 * @param timestamp. The timestamp is used for conflict detection/resolution when two columns with same name need to be compared.
 * @param ttl. An optional, positive delay (in seconds) after which the column will be automatically deleted. 
 */
struct Column {
   1: required binary name,
   2: optional binary value,
   3: optional i64 timestamp,
   4: optional i32 ttl,
}

/** A named list of columns.
 * @param name. see Column.name.
 * @param columns. A collection of standard Columns.  The columns within a super column are defined in an adhoc manner.
 *                 Columns within a super column do not have to have matching structures (similarly named child columns).
 */
struct SuperColumn {
   1: required binary name,
   2: required list<Column> columns,
}

struct CounterColumn {
    1: required binary name,
    2: required i64 value
}

struct CounterSuperColumn {
    1: required binary name,
    2: required list<CounterColumn> columns
}

/**
    Methods for fetching rows/records from Cassandra will return either a single instance of ColumnOrSuperColumn or a list
    of ColumnOrSuperColumns (get_slice()). If you're looking up a SuperColumn (or list of SuperColumns) then the resulting
    instances of ColumnOrSuperColumn will have the requested SuperColumn in the attribute super_column. For queries resulting
    in Columns, those values will be in the attribute column. This change was made between 0.3 and 0.4 to standardize on
    single query methods that may return either a SuperColumn or Column.

    If the query was on a counter column family, you will either get a counter_column (instead of a column) or a 
    counter_super_column (instead of a super_column)

    @param column. The Column returned by get() or get_slice().
    @param super_column. The SuperColumn returned by get() or get_slice().
    @param counter_column. The Counterolumn returned by get() or get_slice().
    @param counter_super_column. The CounterSuperColumn returned by get() or get_slice().
 */
struct ColumnOrSuperColumn {
    1: optional Column column,
    2: optional SuperColumn super_column,
    3: optional CounterColumn counter_column,
    4: optional CounterSuperColumn counter_super_column
}


#
# Exceptions
# (note that internal server errors will raise a TApplicationException, courtesy of Thrift)
#

/** A specific column was requested that does not exist. */
exception NotFoundException {
}

/** Invalid request could mean keyspace or column family does not exist, required parameters are missing, or a parameter is malformed. 
    why contains an associated error message.
*/
exception InvalidRequestException {
    1: required string why
}

/** Not all the replicas required could be created and/or read. */
exception UnavailableException {
}

/** RPC timeout was exceeded.  either a node failed mid-operation, or load was too high, or the requested op was too large. */
exception TimedOutException {
}

/** invalid authentication request (invalid keyspace, user does not exist, or credentials invalid) */
exception AuthenticationException {
    1: required string why
}

/** invalid authorization request (user does not have access to keyspace) */
exception AuthorizationException {
    1: required string why
}

/** schemas are not in agreement across all nodes */
exception SchemaDisagreementException {
}


#
# service api
#
/** 
 * The ConsistencyLevel is an enum that controls both read and write
 * behavior based on the ReplicationFactor of the keyspace.  The
 * different consistency levels have different meanings, depending on
 * if you're doing a write or read operation. 
 *
 * If W + R > ReplicationFactor, where W is the number of nodes to
 * block for on write, and R the number to block for on reads, you
 * will have strongly consistent behavior; that is, readers will
 * always see the most recent write. Of these, the most interesting is
 * to do QUORUM reads and writes, which gives you consistency while
 * still allowing availability in the face of node failures up to half
 * of <ReplicationFactor>. Of course if latency is more important than
 * consistency then you can use lower values for either or both.
 * 
 * Some ConsistencyLevels (ONE, TWO, THREE) refer to a specific number
 * of replicas rather than a logical concept that adjusts
 * automatically with the replication factor.  Of these, only ONE is
 * commonly used; TWO and (even more rarely) THREE are only useful
 * when you care more about guaranteeing a certain level of
 * durability, than consistency.
 * 
 * Write consistency levels make the following guarantees before reporting success to the client:
 *   ANY          Ensure that the write has been written once somewhere, including possibly being hinted in a non-target node.
 *   ONE          Ensure that the write has been written to at least 1 node's commit log and memory table
 *   TWO          Ensure that the write has been written to at least 2 node's commit log and memory table
 *   THREE        Ensure that the write has been written to at least 3 node's commit log and memory table
 *   QUORUM       Ensure that the write has been written to <ReplicationFactor> / 2 + 1 nodes
 *   LOCAL_QUORUM Ensure that the write has been written to <ReplicationFactor> / 2 + 1 nodes, within the local datacenter (requires NetworkTopologyStrategy)
 *   EACH_QUORUM  Ensure that the write has been written to <ReplicationFactor> / 2 + 1 nodes in each datacenter (requires NetworkTopologyStrategy)
 *   ALL          Ensure that the write is written to <code>&lt;ReplicationFactor&gt;</code> nodes before responding to the client.
 * 
 * Read consistency levels make the following guarantees before returning successful results to the client:
 *   ANY          Not supported. You probably want ONE instead.
 *   ONE          Returns the record obtained from a single replica.
 *   TWO          Returns the record with the most recent timestamp once two replicas have replied.
 *   THREE        Returns the record with the most recent timestamp once three replicas have replied.
 *   QUORUM       Returns the record with the most recent timestamp once a majority of replicas have replied.
 *   LOCAL_QUORUM Returns the record with the most recent timestamp once a majority of replicas within the local datacenter have replied.
 *   EACH_QUORUM  Returns the record with the most recent timestamp once a majority of replicas within each datacenter have replied.
 *   ALL          Returns the record with the most recent timestamp once all replicas have replied (implies no replica may be down)..
*/
enum ConsistencyLevel {
    ONE = 1,
    QUORUM = 2,
    LOCAL_QUORUM = 3,
    EACH_QUORUM = 4,
    ALL = 5,
    ANY = 6,
    TWO = 7,
    THREE = 8,
}

/**
    ColumnParent is used when selecting groups of columns from the same ColumnFamily. In directory structure terms, imagine
    ColumnParent as ColumnPath + '/../'.

    See also <a href="cassandra.html#Struct_ColumnPath">ColumnPath</a>
 */
struct ColumnParent {
    3: required string column_family,
    4: optional binary super_column,
}

/** The ColumnPath is the path to a single column in Cassandra. It might make sense to think of ColumnPath and
 * ColumnParent in terms of a directory structure.
 *
 * ColumnPath is used to looking up a single column.
 *
 * @param column_family. The name of the CF of the column being looked up.
 * @param super_column. The super column name.
 * @param column. The column name.
 */
struct ColumnPath {
    3: required string column_family,
    4: optional binary super_column,
    5: optional binary column,
}

/**
    A slice range is a structure that stores basic range, ordering and limit information for a query that will return
    multiple columns. It could be thought of as Cassandra's version of LIMIT and ORDER BY

    @param start. The column name to start the slice with. This attribute is not required, though there is no default value,
                  and can be safely set to '', i.e., an empty byte array, to start with the first column name. Otherwise, it
                  must a valid value under the rules of the Comparator defined for the given ColumnFamily.
    @param finish. The column name to stop the slice at. This attribute is not required, though there is no default value,
                   and can be safely set to an empty byte array to not stop until 'count' results are seen. Otherwise, it
                   must also be a valid value to the ColumnFamily Comparator.
    @param reversed. Whether the results should be ordered in reversed order. Similar to ORDER BY blah DESC in SQL.
    @param count. How many columns to return. Similar to LIMIT in SQL. May be arbitrarily large, but Thrift will
                  materialize the whole result into memory before returning it to the client, so be aware that you may
                  be better served by iterating through slices by passing the last value of one call in as the 'start'
                  of the next instead of increasing 'count' arbitrarily large.
 */
struct SliceRange {
    1: required binary start,
    2: required binary finish,
    3: required bool reversed=0,
    4: required i32 count=100,
}

/**
    A SlicePredicate is similar to a mathematic predicate (see http://en.wikipedia.org/wiki/Predicate_(mathematical_logic)),
    which is described as "a property that the elements of a set have in common."

    SlicePredicate's in Cassandra are described with either a list of column_names or a SliceRange.  If column_names is
    specified, slice_range is ignored.

    @param column_name. A list of column names to retrieve. This can be used similar to Memcached's "multi-get" feature
                        to fetch N known column names. For instance, if you know you wish to fetch columns 'Joe', 'Jack',
                        and 'Jim' you can pass those column names as a list to fetch all three at once.
    @param slice_range. A SliceRange describing how to range, order, and/or limit the slice.
 */
struct SlicePredicate {
    1: optional list<binary> column_names,
    2: optional SliceRange   slice_range,
}

enum IndexOperator {
    EQ,
    GTE,
    GT,
    LTE,
    LT
}

struct IndexExpression {
    1: required binary column_name,
    2: required IndexOperator op,
    3: required binary value,
}

struct IndexClause {
    1: required list<IndexExpression> expressions
    2: required binary start_key,
    3: required i32 count=100,
}

/**
The semantics of start keys and tokens are slightly different.
Keys are start-inclusive; tokens are start-exclusive.  Token
ranges may also wrap -- that is, the end token may be less
than the start one.  Thus, a range from keyX to keyX is a
one-element range, but a range from tokenY to tokenY is the
full ring.
*/
struct KeyRange {
    1: optional binary start_key,
    2: optional binary end_key,
    3: optional string start_token,
    4: optional string end_token,
    5: required i32 count=100
}

/**
    A KeySlice is key followed by the data it maps to. A collection of KeySlice is returned by the get_range_slice operation.

    @param key. a row key
    @param columns. List of data represented by the key. Typically, the list is pared down to only the columns specified by
                    a SlicePredicate.
 */
struct KeySlice {
    1: required binary key,
    2: required list<ColumnOrSuperColumn> columns,
}

struct KeyCount {
    1: required binary key,
    2: required i32 count
}

/**
 * Note that the timestamp is only optional in case of counter deletion.
 */
struct Deletion {
    1: optional i64 timestamp,
    2: optional binary super_column,
    3: optional SlicePredicate predicate,
}

/**
    A Mutation is either an insert (represented by filling column_or_supercolumn) or a deletion (represented by filling the deletion attribute).
    @param column_or_supercolumn. An insert to a column or supercolumn (possibly counter column or supercolumn)
    @param deletion. A deletion of a column or supercolumn
*/
struct Mutation {
    1: optional ColumnOrSuperColumn column_or_supercolumn,
    2: optional Deletion deletion,
}

struct EndpointDetails {
    1: string host,
    2: string datacenter,
    3: optional string rack
}

/**
    A TokenRange describes part of the Cassandra ring, it is a mapping from a range to
    endpoints responsible for that range.
    @param start_token The first token in the range
    @param end_token The last token in the range
    @param endpoints The endpoints responsible for the range (listed by their configured listen_address)
    @param rpc_endpoints The endpoints responsible for the range (listed by their configured rpc_address)
*/
struct TokenRange {
    1: required string start_token,
    2: required string end_token,
    3: required list<string> endpoints,
    4: optional list<string> rpc_endpoints
    5: optional list<EndpointDetails> endpoint_details,
}

/**
    Authentication requests can contain any data, dependent on the IAuthenticator used
*/
struct AuthenticationRequest {
    1: required map<string, string> credentials
}

enum IndexType {
    KEYS,
    CUSTOM
}

/* describes a column in a column family. */
struct ColumnDef {
    1: required binary name,
    2: required string validation_class,
    3: optional IndexType index_type,
    4: optional string index_name,
    5: optional map<string,string> index_options
}


/* describes a column family. */
struct CfDef {
    1: required string keyspace,
    2: required string name,
    3: optional string column_type="Standard",
    5: optional string comparator_type="BytesType",
    6: optional string subcomparator_type,
    8: optional string comment,
    12: optional double read_repair_chance=1.0,
    13: optional list<ColumnDef> column_metadata,
    14: optional i32 gc_grace_seconds,
    15: optional string default_validation_class,
    16: optional i32 id,
    17: optional i32 min_compaction_threshold,
    18: optional i32 max_compaction_threshold,
    24: optional bool replicate_on_write,
    25: optional double merge_shards_chance,
    26: optional string key_validation_class,
    28: optional binary key_alias,
    29: optional string compaction_strategy,
    30: optional map<string,string> compaction_strategy_options,
    32: optional map<string,string> compression_options,
    33: optional double bloom_filter_fp_chance,
}

/* describes a keyspace. */
struct KsDef {
    1: required string name,
    2: required string strategy_class,
    3: optional map<string,string> strategy_options,

    /** @deprecated */
    4: optional i32 replication_factor, 

    5: required list<CfDef> cf_defs,
    6: optional bool durable_writes=1,
}

/** CQL query compression */
enum Compression {
    GZIP = 1,
    NONE = 2
}

enum CqlResultType {
    ROWS = 1,
    VOID = 2,
    INT = 3
}

/** Row returned from a CQL query */
struct CqlRow {
    1: required binary key,
    2: required list<Column> columns
}

struct CqlMetadata {
    1: required map<binary,string> name_types,
    2: required map<binary,string> value_types,
    3: required string default_name_type,
    4: required string default_value_type
}

struct CqlResult {
    1: required CqlResultType type,
    2: optional list<CqlRow> rows,
    3: optional i32 num,
    4: optional CqlMetadata schema
}

struct CqlPreparedResult {
    1: required i32 itemId,
    2: required i32 count
}


service Cassandra {
  # auth methods
  void login(1: required AuthenticationRequest auth_request) throws (1:AuthenticationException authnx, 2:AuthorizationException authzx),
 
  # set keyspace
  void set_keyspace(1: required string keyspace) throws (1:InvalidRequestException ire),
  
  # retrieval methods

  /**
    Get the Column or SuperColumn at the given column_path. If no value is present, NotFoundException is thrown. (This is
    the only method that can throw an exception under non-failure conditions.)
   */
  ColumnOrSuperColumn get(1:required binary key,
                          2:required ColumnPath column_path,
                          3:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
                      throws (1:InvalidRequestException ire, 2:NotFoundException nfe, 3:UnavailableException ue, 4:TimedOutException te),

  /**
    Get the group of columns contained by column_parent (either a ColumnFamily name or a ColumnFamily/SuperColumn name
    pair) specified by the given SlicePredicate. If no matching values are found, an empty list is returned.
   */
  list<ColumnOrSuperColumn> get_slice(1:required binary key, 
                                      2:required ColumnParent column_parent, 
                                      3:required SlicePredicate predicate, 
                                      4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
                            throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    returns the number of columns matching <code>predicate</code> for a particular <code>key</code>, 
    <code>ColumnFamily</code> and optionally <code>SuperColumn</code>.
  */
  i32 get_count(1:required binary key, 
                2:required ColumnParent column_parent, 
                3:required SlicePredicate predicate,
                4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
      throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Performs a get_slice for column_parent and predicate for the given keys in parallel.
  */
  map<binary,list<ColumnOrSuperColumn>> multiget_slice(1:required list<binary> keys, 
                                                       2:required ColumnParent column_parent, 
                                                       3:required SlicePredicate predicate, 
                                                       4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
                                        throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Perform a get_count in parallel on the given list<binary> keys. The return value maps keys to the count found.
  */
  map<binary, i32> multiget_count(1:required list<binary> keys,
                2:required ColumnParent column_parent,
                3:required SlicePredicate predicate,
                4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
      throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
   returns a subset of columns for a contiguous range of keys.
  */
  list<KeySlice> get_range_slices(1:required ColumnParent column_parent, 
                                  2:required SlicePredicate predicate,
                                  3:required KeyRange range,
                                  4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
                 throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /** Returns the subset of columns specified in SlicePredicate for the rows matching the IndexClause */
  list<KeySlice> get_indexed_slices(1:required ColumnParent column_parent,
                                    2:required IndexClause index_clause,
                                    3:required SlicePredicate column_predicate,
                                    4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
                 throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  # modification methods

  /**
   * Insert a Column at the given column_parent.column_family and optional column_parent.super_column.
   */
  void insert(1:required binary key, 
              2:required ColumnParent column_parent,
              3:required Column column,
              4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
   * Increment or decrement a counter.
   */
  void add(1:required binary key,
           2:required ColumnParent column_parent,
           3:required CounterColumn column,
           4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Remove data from the row specified by key at the granularity specified by column_path, and the given timestamp. Note
    that all the values in column_path besides column_path.column_family are truly optional: you can remove the entire
    row by just specifying the ColumnFamily, or you can remove a SuperColumn or a single Column by specifying those levels too.
   */
  void remove(1:required binary key,
              2:required ColumnPath column_path,
              3:required i64 timestamp,
              4:ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
   * Remove a counter at the specified location.
   * Note that counters have limited support for deletes: if you remove a counter, you must wait to issue any following update
   * until the delete has reached all the nodes and all of them have been fully compacted.
   */
  void remove_counter(1:required binary key,
                      2:required ColumnPath path,
                      3:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
      throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),


  /**
    Mutate many columns or super columns for many row keys. See also: Mutation.

    mutation_map maps key to column family to a list of Mutation objects to take place at that scope.
  **/
  void batch_mutate(1:required map<binary, map<string, list<Mutation>>> mutation_map,
                    2:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),
       
  /**
   Truncate will mark and entire column family as deleted.
   From the user's perspective a successful call to truncate will result complete data deletion from cfname.
   Internally, however, disk space will not be immediatily released, as with all deletes in cassandra, this one
   only marks the data as deleted.
   The operation succeeds only if all hosts in the cluster at available and will throw an UnavailableException if 
   some hosts are down.
  */
  void truncate(1:required string cfname)
       throws (1: InvalidRequestException ire, 2: UnavailableException ue, 3: TimedOutException te),


    
  // Meta-APIs -- APIs to get information about the node or cluster,
  // rather than user data.  The nodeprobe program provides usage examples.
  
  /** 
   * for each schema version present in the cluster, returns a list of nodes at that version.
   * hosts that do not respond will be under the key DatabaseDescriptor.INITIAL_VERSION. 
   * the cluster is all on the same version if the size of the map is 1. 
   */
  map<string, list<string>> describe_schema_versions()
       throws (1: InvalidRequestException ire),

  /** list the defined keyspaces in this cluster */
  list<KsDef> describe_keyspaces()
    throws (1:InvalidRequestException ire),

  /** get the cluster name */
  string describe_cluster_name(),

  /** get the thrift api version */
  string describe_version(),

  /** get the token ring: a map of ranges to host addresses,
      represented as a set of TokenRange instead of a map from range
      to list of endpoints, because you can't use Thrift structs as
      map keys:
      https://issues.apache.org/jira/browse/THRIFT-162 

      for the same reason, we can't return a set here, even though
      order is neither important nor predictable. */
  list<TokenRange> describe_ring(1:required string keyspace)
                   throws (1:InvalidRequestException ire),

  /** returns the partitioner used by this cluster */
  string describe_partitioner(),

  /** returns the snitch used by this cluster */
  string describe_snitch(),

  /** describe specified keyspace */
  KsDef describe_keyspace(1:required string keyspace)
    throws (1:NotFoundException nfe, 2:InvalidRequestException ire),

  /** experimental API for hadoop/parallel query support.  
      may change violently and without warning. 

      returns list of token strings such that first subrange is (list[0], list[1]],
      next is (list[1], list[2]], etc. */
  list<string> describe_splits(1:required string cfName,
                               2:required string start_token, 
                               3:required string end_token,
                               4:required i32 keys_per_split)
    throws (1:InvalidRequestException ire),

  /** adds a column family. returns the new schema id. */
  string system_add_column_family(1:required CfDef cf_def)
    throws (1:InvalidRequestException ire, 2:SchemaDisagreementException sde),
    
  /** drops a column family. returns the new schema id. */
  string system_drop_column_family(1:required string column_family)
    throws (1:InvalidRequestException ire, 2:SchemaDisagreementException sde), 
  
  /** adds a keyspace and any column families that are part of it. returns the new schema id. */
  string system_add_keyspace(1:required KsDef ks_def)
    throws (1:InvalidRequestException ire, 2:SchemaDisagreementException sde),
  
  /** drops a keyspace and any column families that are part of it. returns the new schema id. */
  string system_drop_keyspace(1:required string keyspace)
    throws (1:InvalidRequestException ire, 2:SchemaDisagreementException sde),
  
  /** updates properties of a keyspace. returns the new schema id. */
  string system_update_keyspace(1:required KsDef ks_def)
    throws (1:InvalidRequestException ire, 2:SchemaDisagreementException sde),
        
  /** updates properties of a column family. returns the new schema id. */
  string system_update_column_family(1:required CfDef cf_def)
    throws (1:InvalidRequestException ire, 2:SchemaDisagreementException sde),
  
  /**
   * Executes a CQL (Cassandra Query Language) statement and returns a
   * CqlResult containing the results.
   */
  CqlResult execute_cql_query(1:required binary query, 2:required Compression compression)
    throws (1:InvalidRequestException ire,
            2:UnavailableException ue,
            3:TimedOutException te,
            4:SchemaDisagreementException sde)
            
            
  /**
   * Prepare a CQL (Cassandra Query Language) statement by compiling and returning
   * - the type of CQL statement
   * - an id token of the compiled CQL stored on the server side.
   * - a count of the discovered bound markers in the statement 
   */
  CqlPreparedResult prepare_cql_query(1:required binary query, 2:required Compression compression)
    throws (1:InvalidRequestException ire)

             
  /**
   * Executes a prepared CQL (Cassandra Query Language) statement by passing an id token and  a list of variables
   * to bind and returns a CqlResult containing the results.
   */
  CqlResult execute_prepared_cql_query(1:required i32 itemId, 2:required list<string> values)
    throws (1:InvalidRequestException ire,
            2:UnavailableException ue,
            3:TimedOutException te,
            4:SchemaDisagreementException sde)
           

}
