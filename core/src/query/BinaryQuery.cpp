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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "query/BinaryQuery.h"

namespace milvus {
namespace query {

BinaryQueryPtr
ConstructBinTree(std::vector<BooleanQueryPtr> queries, QueryRelation relation, uint64_t idx) {
    if (idx == queries.size()) {
        return nullptr;
    } else if (idx == queries.size() - 1) {
        return queries[idx]->getBinaryQuery();
    } else {
        BinaryQueryPtr bquery = std::make_shared<BinaryQuery>();
        bquery->relation = relation;
        bquery->left_query = std::make_shared<GeneralQuery>();
        bquery->right_query = std::make_shared<GeneralQuery>();
        bquery->left_query->bin = queries[idx]->getBinaryQuery();
        ++idx;
        bquery->right_query->bin = ConstructBinTree(queries, relation, idx);
        return bquery;
    }
}

Status
ConstructLeafBinTree(std::vector<LeafQueryPtr> leaf_queries, BinaryQueryPtr binary_query, uint64_t idx) {
    if (idx == leaf_queries.size()) {
        return Status::OK();
    }
    binary_query->left_query = std::make_shared<GeneralQuery>();
    binary_query->right_query = std::make_shared<GeneralQuery>();
    if (leaf_queries.size() == leaf_queries.size() - 1) {
        binary_query->left_query->leaf = leaf_queries[idx];
        return Status::OK();
    } else if (idx == leaf_queries.size() - 2) {
        binary_query->left_query->leaf = leaf_queries[idx];
        ++idx;
        binary_query->right_query->leaf = leaf_queries[idx];
        return Status::OK();
    } else {
        binary_query->left_query->bin->relation = binary_query->relation;
        binary_query->right_query->leaf = leaf_queries[idx];
        ++idx;
        return ConstructLeafBinTree(leaf_queries, binary_query->left_query->bin, idx);
    }
}

Status
GenBinaryQuery(BooleanQueryPtr query, BinaryQueryPtr& binary_query) {
    if (query->getBooleanQuerys().size() == 0) {
        if (binary_query->relation == QueryRelation::AND || binary_query->relation == QueryRelation::OR) {
            // Put VectorQuery to the end of leafqueries
            auto query_size = query->getLeafQueries().size();
            for (uint64_t i = 0; i < query_size; ++i) {
                if (query->getLeafQueries()[i]->vector_query != nullptr) {
                    std::swap(query->getLeafQueries()[i], query->getLeafQueries()[0]);
                    break;
                }
            }
            return ConstructLeafBinTree(query->getLeafQueries(), binary_query, 0);
        } else {
            switch (query->getOccur()) {
                case Occur::MUST: {
                    binary_query->relation = QueryRelation::AND;
                    return GenBinaryQuery(query, binary_query);
                }
                case Occur::MUST_NOT:
                case Occur::SHOULD: {
                    binary_query->relation = QueryRelation::OR;
                    return GenBinaryQuery(query, binary_query);
                }
            }
        }
    }

    if (query->getBooleanQuerys().size() == 1) {
        auto bc = query->getBooleanQuerys()[0];
        binary_query->left_query = std::make_shared<GeneralQuery>();
        switch (bc->getOccur()) {
            case Occur::MUST: {
                binary_query->relation = QueryRelation::AND;
                Status s = GenBinaryQuery(bc, binary_query);
                return s;
            }
            case Occur::MUST_NOT:
            case Occur::SHOULD: {
                binary_query->relation = QueryRelation::OR;
                Status s = GenBinaryQuery(bc, binary_query);
                return s;
            }
        }
    }

    // Construct binary query for every single boolean query
    std::vector<BooleanQueryPtr> must_queries;
    std::vector<BooleanQueryPtr> must_not_queries;
    std::vector<BooleanQueryPtr> should_queries;
    Status status;
    for (auto& _query : query->getBooleanQuerys()) {
        status = GenBinaryQuery(_query, _query->getBinaryQuery());
        if (!status.ok()) {
            return status;
        }
        if (_query->getOccur() == Occur::MUST) {
            must_queries.emplace_back(_query);
        } else if (_query->getOccur() == Occur::MUST_NOT) {
            must_not_queries.emplace_back(_query);
        } else {
            should_queries.emplace_back(_query);
        }
    }

    // Construct binary query for combine boolean queries
    BinaryQueryPtr must_bquery, should_bquery, must_not_bquery;
    uint64_t bquery_num = 0;
    if (must_queries.size() > 1) {
        // Construct a must binary tree
        must_bquery = ConstructBinTree(must_queries, QueryRelation::R1, 0);
        ++bquery_num;
    } else if (must_queries.size() == 1) {
        must_bquery = must_queries[0]->getBinaryQuery();
        ++bquery_num;
    }

    if (should_queries.size() > 1) {
        // Construct a should binary tree
        should_bquery = ConstructBinTree(should_queries, QueryRelation::R2, 0);
        ++bquery_num;
    } else if (should_queries.size() == 1) {
        should_bquery = should_queries[0]->getBinaryQuery();
        ++bquery_num;
    }

    if (must_not_queries.size() > 1) {
        // Construct a must_not binary tree
        must_not_bquery = ConstructBinTree(must_not_queries, QueryRelation::R1, 0);
        ++bquery_num;
    } else if (must_not_queries.size() == 1) {
        must_not_bquery = must_not_queries[0]->getBinaryQuery();
        ++bquery_num;
    }

    binary_query->left_query = std::make_shared<GeneralQuery>();
    binary_query->right_query = std::make_shared<GeneralQuery>();
    BinaryQueryPtr must_should_query = std::make_shared<BinaryQuery>();
    must_should_query->left_query = std::make_shared<GeneralQuery>();
    must_should_query->right_query = std::make_shared<GeneralQuery>();
    if (bquery_num == 3) {
        must_should_query->relation = QueryRelation::R3;
        must_should_query->left_query->bin = must_bquery;
        must_should_query->right_query->bin = should_bquery;
        binary_query->relation = QueryRelation::R1;
        binary_query->left_query->bin = must_should_query;
        binary_query->right_query->bin = must_not_bquery;
    } else if (bquery_num == 2) {
        if (must_bquery == nullptr) {
            binary_query->relation = QueryRelation::R3;
            binary_query->left_query->bin = must_not_bquery;
            binary_query->right_query->bin = should_bquery;
        } else if (should_bquery == nullptr) {
            binary_query->relation = QueryRelation::R4;
            binary_query->left_query->bin = must_bquery;
            binary_query->right_query->bin = must_not_bquery;
        } else {
            binary_query->relation = QueryRelation::R3;
            binary_query->left_query->bin = must_bquery;
            binary_query->right_query->bin = should_bquery;
        }
    } else {
        if (must_bquery != nullptr) {
            binary_query = must_bquery;
        } else if (should_bquery != nullptr) {
            binary_query = should_bquery;
        } else {
            binary_query = must_not_bquery;
        }
    }

    return Status::OK();
}

uint64_t
BinaryQueryHeight(BinaryQueryPtr& binary_query) {
    if (binary_query == nullptr) {
        return 1;
    }
    uint64_t left_height = 0, right_height = 0;
    if (binary_query->left_query != nullptr) {
        left_height = BinaryQueryHeight(binary_query->left_query->bin);
    }
    if (binary_query->right_query != nullptr) {
        right_height = BinaryQueryHeight(binary_query->right_query->bin);
    }
    return left_height > right_height ? left_height + 1 : right_height + 1;
}

bool
ValidateBinaryQuery(BinaryQueryPtr& binary_query) {
    // Only for one layer BooleanQuery
    uint64_t height = BinaryQueryHeight(binary_query);
    return height > 1 && height < 4;
}

}  // namespace query
}  // namespace milvus
