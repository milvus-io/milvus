#pragma once
#include <cstdint>
#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include <string>


namespace milvus::storage{
namespace plugin{

using ByteArray = std::string;

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

    virtual void Update(int64_t ez_id, int64_t coll_id, const ByteArray& key) = 0;

    virtual std::pair<std::shared_ptr<IEncryptor>, ByteArray>
    GetEncryptor(int64_t ez_id, int64_t coll_id) = 0;

    virtual std::shared_ptr<IDecryptor>
    GetDecryptor(int64_t ez_id, int64_t coll_id, const std::string& safeKey) = 0;
};

class IEncryptor {
public:
    virtual ~IEncryptor() = default;
    virtual ByteArray
    Encrypt(const ByteArray& plaintext) = 0;
};

class IDecryptor {
public:
    virtual ~IDecryptor() = default;
    virtual ByteArray
    Decrypt(const ByteArray& ciphertext) = 0;
};


} // namspace plugin
} // namespace milvus::storage
