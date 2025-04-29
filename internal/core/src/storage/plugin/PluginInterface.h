#pragma once
#include <cstdint>
#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include <string>


namespace milvus::storage{
namespace plugin{

class IEncryptor;
class IDecryptor;
class ICipherPlugin;

class IPlugin{
public:
    virtual ~IPlugin() = default;

    virtual std::string
    getPluginName() const = 0;
};

class ICipherPlugin: public IPlugin{
public:
    virtual ~ICipherPlugin() = default;

    std::string
    getPluginName() const override{ return "ICipherPlugin"; }

    virtual void Update(int64_t ez_id, int64_t coll_id, const std::string& key) = 0;

    virtual std::pair<std::shared_ptr<IEncryptor>, std::string>
    GetEncryptor(int64_t ez_id, int64_t coll_id) = 0;

    virtual std::shared_ptr<IDecryptor>
    GetDecryptor(int64_t ez_id, int64_t coll_id, const std::string& safeKey) = 0;
};

class IEncryptor {
public:
    virtual ~IEncryptor() = default;
    virtual std::string
    Encrypt(const std::string& plaintext) = 0;

    virtual std::string
    GetKey() = 0;
};

class IDecryptor {
public:
    virtual ~IDecryptor() = default;
    virtual std::string
    Decrypt(const std::string& ciphertext) = 0;

    virtual std::string
    GetKey() = 0;
};


} // namspace plugin
} // namespace milvus::storage
