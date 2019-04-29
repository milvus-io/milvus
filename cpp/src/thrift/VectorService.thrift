/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

enum VecErrCode {
    SUCCESS = 0,
	ILLEGAL_ARGUMENT,
	GROUP_NOT_EXISTS,
	ILLEGAL_TIME_RANGE,
	ILLEGAL_VECTOR_DIMENSION,
	OUT_OF_MEMORY,
}

exception VecException {
	1: VecErrCode code;
	2: string reason;
}

struct VecGroup {
	1: required string id;
	2: required i32 dimension;
	3: optional i32 index_type;
}

struct VecTensor {
    1: required string uid;
	2: required list<double> tensor;
	3: optional map<string, string> attrib;
}

struct VecTensorList {
	1: required list<VecTensor> tensor_list;
}

struct VecBinaryTensor {
    1: required string uid;
	2: required binary tensor;
	3: optional map<string, string> attrib;
}

struct VecBinaryTensorList {
	1: required list<VecBinaryTensor> tensor_list;
}

struct VecSearchResultItem {
    1: required string uid;
    2: optional double distance;
    3: optional map<string, string> attrib;
}

struct VecSearchResult {
    1: list<VecSearchResultItem> result_list;
}

struct VecSearchResultList {
    1: list<VecSearchResult> result_list;
}


struct VecDateTime {
    1: required i32 year;
    2: required i32 month;
    3: required i32 day;
    4: required i32 hour;
    5: required i32 minute;
    6: required i32 second;
}

struct VecTimeRange {
    1: required VecDateTime time_begin;
    2: required bool begine_closed;
    3: required VecDateTime time_end;
    4: required bool end_closed;
}

struct VecSearchFilter {
    1: optional map<string, string> attrib_filter;
    2: optional list<VecTimeRange> time_ranges;
}

service VecService {
    /**
     * group interfaces
     */
	void add_group(2: VecGroup group) throws(1: VecException e);
	VecGroup get_group(2: string group_id) throws(1: VecException e);
	void del_group(2: string group_id) throws(1: VecException e);


    /**
     * insert vector interfaces
     *
     */
    void add_vector(2: string group_id, 3: VecTensor tensor) throws(1: VecException e);
    void add_vector_batch(2: string group_id, 3: VecTensorList tensor_list) throws(1: VecException e);
    void add_binary_vector(2: string group_id, 3: VecBinaryTensor tensor) throws(1: VecException e);
    void add_binary_vector_batch(2: string group_id, 3: VecBinaryTensorList tensor_list) throws(1: VecException e);

    /**
     * search interfaces
     * you can use filter to reduce search result
     * filter.attrib_filter can specify which attribute you need, for example:
     * set attrib_filter = {"color":""} means you want to get "color" attribute for result vector
     * set attrib_filter = {"color":"red"} means you want to get vectors which has attribute "color" equals "red"
     * if filter.time_range is empty, engine will search without time limit
     */
    VecSearchResult search_vector(2: string group_id, 3: i64 top_k, 4: VecTensor tensor, 5: VecSearchFilter filter) throws(1: VecException e);
    VecSearchResultList search_vector_batch(2: string group_id, 3: i64 top_k, 4: VecTensorList tensor_list, 5: VecSearchFilter filter) throws(1: VecException e);
    VecSearchResult search_binary_vector(2: string group_id, 3: i64 top_k, 4: VecBinaryTensor tensor, 5: VecSearchFilter filter) throws(1: VecException e);
    VecSearchResultList search_binary_vector_batch(2: string group_id, 3: i64 top_k, 4: VecBinaryTensorList tensor_list, 5: VecSearchFilter filter) throws(1: VecException e);
}