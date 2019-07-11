/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Options.h"
#include "MetaTypes.h"

#include <string>

namespace zilliz {
namespace milvus {
namespace engine {
namespace utils {

long GetMicroSecTimeStamp();

Status CreateTablePath(const DBMetaOptions& options, const std::string& table_id);
Status DeleteTablePath(const DBMetaOptions& options, const std::string& table_id);

Status CreateTableFilePath(const DBMetaOptions& options, meta::TableFileSchema& table_file);
Status GetTableFilePath(const DBMetaOptions& options, meta::TableFileSchema& table_file);
Status DeleteTableFilePath(const DBMetaOptions& options, meta::TableFileSchema& table_file);

} // namespace utils
} // namespace engine
} // namespace milvus
} // namespace zilliz
