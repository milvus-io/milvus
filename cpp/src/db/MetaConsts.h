/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

namespace zilliz {
namespace vecwise {
namespace engine {
namespace meta {

const size_t K = 1024UL;
const size_t M = K*K;
const size_t G = K*M;
const size_t T = K*G;

const size_t S_PS = 1UL;
const size_t MS_PS = 1000*S_PS;
const size_t US_PS = 1000*MS_PS;
const size_t NS_PS = 1000*US_PS;

const size_t SECOND = 1UL;
const size_t M_SEC = 60*SECOND;
const size_t H_SEC = 60*M_SEC;
const size_t D_SEC = 24*H_SEC;
const size_t W_SEC = 7*D_SEC;

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
