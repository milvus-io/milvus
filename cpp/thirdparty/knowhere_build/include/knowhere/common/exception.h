////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <exception>
#include <string>


namespace zilliz {
namespace knowhere {

class KnowhereException : public std::exception {
 public:
    explicit KnowhereException(const std::string &msg);

    KnowhereException(const std::string &msg, const char *funName,
                      const char *file, int line);

    const char *what() const noexcept override;

    std::string msg;
};


#define KNOWHERE_THROW_MSG(MSG)\
do {\
    throw KnowhereException(MSG, __PRETTY_FUNCTION__, __FILE__, __LINE__);\
} while (false)

#define KNOHERE_THROW_FORMAT(FMT, ...)\
    do { \
    std::string __s;\
    int __size = snprintf(nullptr, 0, FMT, __VA_ARGS__);\
    __s.resize(__size + 1);\
    snprintf(&__s[0], __s.size(), FMT, __VA_ARGS__);\
    throw faiss::FaissException(__s, __PRETTY_FUNCTION__, __FILE__, __LINE__);\
   } while (false)


}
}