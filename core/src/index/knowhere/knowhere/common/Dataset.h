// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <typeindex>
#include <utility>

namespace knowhere {

struct BaseValue;
using BasePtr = std::unique_ptr<BaseValue>;
struct BaseValue {
    virtual ~BaseValue() = default;

    //    virtual BasePtr
    //    Clone() const = 0;
};

template <typename T>
struct AnyValue : public BaseValue {
    T data_;

    template <typename U>
    explicit AnyValue(U&& value) : data_(std::forward<U>(value)) {
    }

    //    BasePtr
    //    Clone() const {
    //        return BasePtr(data_);
    //    }
};

struct Value {
    std::type_index type_;
    BasePtr data_;

    template <typename U,
              class = typename std::enable_if<!std::is_same<typename std::decay<U>::type, Value>::value, U>::type>
    explicit Value(U&& value)
        : data_(new AnyValue<typename std::decay<U>::type>(std::forward<U>(value))),
          type_(std::type_index(typeid(typename std::decay<U>::type))) {
    }

    template <typename U>
    bool
    Is() const {
        return type_ == std::type_index(typeid(U));
    }

    template <typename U>
    U&
    AnyCast() {
        if (!Is<U>()) {
            std::stringstream ss;
            ss << "Can't cast t " << type_.name() << " to " << typeid(U).name();
            throw std::logic_error(ss.str());
        }

        auto derived = dynamic_cast<AnyValue<U>*>(data_.get());
        return derived->data_;
    }
};
using ValuePtr = std::shared_ptr<Value>;

class Dataset {
 public:
    Dataset() = default;

    template <typename T>
    void
    Set(const std::string& k, T&& v) {
        std::lock_guard<std::mutex> lk(mutex_);
        auto value = std::make_shared<Value>(std::forward<T>(v));
        data_[k] = value;
    }

    template <typename T>
    T
    Get(const std::string& k) {
        std::lock_guard<std::mutex> lk(mutex_);
        auto finder = data_.find(k);
        if (finder != data_.end()) {
            return finder->second->AnyCast<T>();
        } else {
            throw std::logic_error("Can't find this key");
        }
    }

    const std::map<std::string, ValuePtr>&
    data() const {
        return data_;
    }

 private:
    std::mutex mutex_;
    std::map<std::string, ValuePtr> data_;
};
using DatasetPtr = std::shared_ptr<Dataset>;

}  // namespace knowhere
