// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/string.hpp>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/nvp.hpp>
#include <boost/serialization/split_free.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/serialization/vector.hpp>

class RTreeSerializer {
 public:
    template <typename RTreeType>
    static bool
    saveBinary(const RTreeType& tree, const std::string& filename) {
        try {
            std::ofstream ofs(filename, std::ios::binary);
            if (!ofs.is_open()) {
                std::cerr << "Cannot open file for writing: " << filename
                          << std::endl;
                return false;
            }

            boost::archive::binary_oarchive oa(ofs);
            oa << tree;

            ofs.close();
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Serialization error: " << e.what() << std::endl;
            return false;
        }
    }

    template <typename RTreeType>
    static bool
    loadBinary(RTreeType& tree, const std::string& filename) {
        try {
            std::ifstream ifs(filename, std::ios::binary);
            if (!ifs.is_open()) {
                std::cerr << "Cannot open file for reading: " << filename
                          << std::endl;
                return false;
            }

            boost::archive::binary_iarchive ia(ifs);
            ia >> tree;

            ifs.close();
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Deserialization error: " << e.what() << std::endl;
            return false;
        }
    }

    template <typename RTreeType>
    static bool
    saveText(const RTreeType& tree, const std::string& filename) {
        try {
            std::ofstream ofs(filename);
            if (!ofs.is_open()) {
                std::cerr << "Cannot open file for writing: " << filename
                          << std::endl;
                return false;
            }

            boost::archive::text_oarchive oa(ofs);
            oa << tree;

            ofs.close();
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Serialization error: " << e.what() << std::endl;
            return false;
        }
    }

    template <typename RTreeType>
    static bool
    loadText(RTreeType& tree, const std::string& filename) {
        try {
            std::ifstream ifs(filename);
            if (!ifs.is_open()) {
                std::cerr << "Cannot open file for reading: " << filename
                          << std::endl;
                return false;
            }

            boost::archive::text_iarchive ia(ifs);
            ia >> tree;

            ifs.close();
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Deserialization error: " << e.what() << std::endl;
            return false;
        }
    }

    template <typename RTreeType>
    static std::string
    serializeToString(const RTreeType& tree) {
        std::ostringstream oss;
        boost::archive::binary_oarchive oa(oss);
        oa << tree;
        return oss.str();
    }

    template <typename RTreeType>
    static bool
    deserializeFromString(RTreeType& tree, const std::string& data) {
        try {
            std::istringstream iss(data);
            boost::archive::binary_iarchive ia(iss);
            ia >> tree;
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Deserialization error: " << e.what() << std::endl;
            return false;
        }
    }
};
