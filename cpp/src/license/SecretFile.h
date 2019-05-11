/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once
#include <string>

class SecretFile {
 public:
    SecretFile() = default;

    SecretFile(const time_t &update_time, const off_t &file_size, const std::string &file_md5)
        : update_time_(update_time), file_size_(file_size), file_md5_(file_md5) {}

    time_t get_update_time() {
        return update_time_;
    }
    off_t get_file_size() {
        return file_size_;
    }
    std::string get_file_md5() {
        return file_md5_;
    }

 private:
    friend class boost::serialization::access;

    template<typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & update_time_;
        ar & file_size_;
        ar & file_md5_;
    }

 public:
    time_t update_time_ = 0;
    off_t file_size_ = 0;
    std::string file_md5_;
};

class SerializedSecretFile {
 public:
    ~SerializedSecretFile() {
        if(secret_file_ != nullptr) {
            delete secret_file_;
            secret_file_ = nullptr;
        }
    }

    void
    set_secret_file(SecretFile* secret_file) {
        secret_file_ = secret_file;
    }

    SecretFile* get_secret_file() {
        return secret_file_;
    }

 private:
    friend class boost::serialization::access;

    template<typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & secret_file_;
    }

 private:
    SecretFile *secret_file_ = nullptr;
};


