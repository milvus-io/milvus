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
	1: string id;
	2: i32 dimension;
	3: i32 index_type;
}

struct VecTensor {
    1: string uid;
	2: list<double> tensor;
}

struct VecTensorList {
	1: list<VecTensor> tensor_list;
}

struct VecSearchResult {
    1: list<string> id_list;
}

struct VecSearchResultList {
    1: list<VecSearchResult> result_list;
}


struct VecDateTime {
    1: i32 year;
    2: i32 month;
    3: i32 day;
    4: i32 hour;
    5: i32 minute;
    6: i32 second;
}

struct VecTimeRange {
    1: VecDateTime time_begin;
    2: bool begine_closed;
    3: VecDateTime time_end;
    4: bool end_closed;
}

struct VecTimeRangeList {
    1: list<VecTimeRange> range_list;
}

service VecService {
    /**
     * group interfaces
     */
	void add_group(2: VecGroup group) throws(1: VecException e);
	VecGroup get_group(2: string group_id) throws(1: VecException e);
	void del_group(2: string group_id) throws(1: VecException e);


    /**
     * vector interfaces
     *
     */
    void add_vector(2: string group_id, 3: VecTensor tensor) throws(1: VecException e);
    void add_vector_batch(2: string group_id, 3: VecTensorList tensor_list) throws(1: VecException e);

    /**
     * search interfaces
     * if time_range_list is empty, engine will search without time limit
     */
    VecSearchResult search_vector(2: string group_id, 3: i64 top_k, 4: VecTensor tensor, 5: VecTimeRangeList time_range_list) throws(1: VecException e);
    VecSearchResultList search_vector_batch(2: string group_id, 3: i64 top_k, 4: VecTensorList tensor_list, 5: VecTimeRangeList time_range_list) throws(1: VecException e);
}