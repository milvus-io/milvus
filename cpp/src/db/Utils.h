/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Options.h"
#include "db/meta/MetaTypes.h"
#include "db/Types.h"

#include <string>

namespace zilliz {
namespace milvus {
namespace engine {
namespace utils {

long GetMicroSecTimeStamp();

Status CreateTablePath(const DBMetaOptions& options, const std::string& table_id);
Status DeleteTablePath(const DBMetaOptions& options, const std::string& table_id, bool force = true);

Status CreateTableFilePath(const DBMetaOptions& options, meta::TableFileSchema& table_file);
Status GetTableFilePath(const DBMetaOptions& options, meta::TableFileSchema& table_file);
Status DeleteTableFilePath(const DBMetaOptions& options, meta::TableFileSchema& table_file);

bool IsSameIndex(const TableIndex& index1, const TableIndex& index2);

bool UserDefinedId(int64_t flag);

} // namespace utils
} // namespace engine
} // namespace milvus
} // namespace zilliz
