/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

namespace zilliz {
namespace vecwise {
namespace engine {

const char* IVFIndexTrait::BuildIndexType = "IVF";
const char* IVFIndexTrait::RawIndexType = "IDMap,Flat";

const char* IDMapIndexTrait::BuildIndexType = "IDMap";
const char* IDMapIndexTrait::RawIndexType = "IDMap,Flat";

} // namespace engine
} // namespace vecwise
} // namespace zilliz
