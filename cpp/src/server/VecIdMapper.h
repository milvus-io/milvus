/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "utils/Error.h"

#include <string>
#include <vector>
#include <unordered_map>

namespace zilliz {
namespace vecwise {
namespace server {

using INTEGER_ID = int64_t;

class IVecIdMapper {
public:
    static IVecIdMapper* GetInstance();

    virtual ~IVecIdMapper(){}

    virtual ServerError Put(INTEGER_ID nid, const std::string& sid) = 0;
    virtual ServerError Put(const std::vector<INTEGER_ID>& nid, const std::vector<std::string>& sid) = 0;
    virtual ServerError Put(const INTEGER_ID *nid, uint64_t count, const std::vector<std::string>& sid) = 0;

    virtual ServerError Get(INTEGER_ID nid, std::string& sid) const = 0;
    virtual ServerError Get(const std::vector<INTEGER_ID>& nid, std::vector<std::string>& sid) const = 0;
    virtual ServerError Get(const INTEGER_ID *nid, uint64_t count, std::vector<std::string>& sid) const = 0;

    virtual ServerError Delete(INTEGER_ID nid) = 0;
};

class SimpleIdMapper : public IVecIdMapper{
public:
    SimpleIdMapper();
    ~SimpleIdMapper();

    ServerError Put(INTEGER_ID nid, const std::string& sid) override;
    ServerError Put(const std::vector<INTEGER_ID>& nid, const std::vector<std::string>& sid) override;
    ServerError Put(const INTEGER_ID *nid, uint64_t count, const std::vector<std::string>& sid) override;

    ServerError Get(INTEGER_ID nid, std::string& sid) const override;
    ServerError Get(const std::vector<INTEGER_ID>& nid, std::vector<std::string>& sid) const override;
    ServerError Get(const INTEGER_ID *nid, uint64_t count, std::vector<std::string>& sid) const override;

    ServerError Delete(INTEGER_ID nid) override;

private:
    std::unordered_map<INTEGER_ID, std::string> ids_;
};

class RocksIdMapper : public IVecIdMapper{
public:
    RocksIdMapper(const std::string& store_path);
    ~RocksIdMapper();

    ServerError Put(INTEGER_ID nid, const std::string& sid) override;
    ServerError Put(const std::vector<INTEGER_ID>& nid, const std::vector<std::string>& sid) override;
    ServerError Put(const INTEGER_ID *nid, uint64_t count, const std::vector<std::string>& sid) override;

    ServerError Get(INTEGER_ID nid, std::string& sid) const override;
    ServerError Get(const std::vector<INTEGER_ID>& nid, std::vector<std::string>& sid) const override;
    ServerError Get(const INTEGER_ID *nid, uint64_t count, std::vector<std::string>& sid) const override;

    ServerError Delete(INTEGER_ID nid) override;

};

}
}
}
