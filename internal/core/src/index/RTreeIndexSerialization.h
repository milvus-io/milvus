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

#include <atomic>
#include <fstream>
#include <iostream>
#include <new>
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
    enum class BinaryIOResult {
        Success,
        OpenFailed,
        StreamFailed,
        ArchiveFailed,
    };

    /**
     * Test-only one-shot fault injection for the close() check in saveBinary.
     *
     * A close(2) that fails after a successful flush needs a filesystem that
     * defers ENOSPC/EIO to close (ext4 delalloc, XFS, NFS) on a full or
     * failing device; a unit test cannot provoke that, so the branch would
     * otherwise be unreachable from a test. Production code never sets it.
     */
    static std::atomic<bool>&
    CloseFailureForTesting() {
        static std::atomic<bool> flag{false};
        return flag;
    }

    template <typename RTreeType>
    static BinaryIOResult
    saveBinary(const RTreeType& tree, const std::string& filename) {
        try {
            std::ofstream ofs(filename, std::ios::binary);
            if (!ofs.is_open()) {
                return BinaryIOResult::OpenFailed;
            }

            {
                boost::archive::binary_oarchive oa(ofs);
                oa << tree;
            }
            ofs.flush();
            if (!ofs.good()) {
                return BinaryIOResult::StreamFailed;
            }
            // close() explicitly, and check it. flush() only guarantees the
            // streambuf reached write(2); on a delayed-allocation filesystem
            // (ext4 delalloc, XFS) ENOSPC/EIO for those blocks is reported at
            // close(2). Letting ~basic_ofstream do the closing swallows that
            // failbit, so a truncated .bgi would be reported as Success and
            // uploaded as a successfully built index -- the exact outcome
            // finish() must never produce -- resurfacing much later at load
            // as ArchiveFailed/DataFormatBroken.
            ofs.close();
            if (CloseFailureForTesting().exchange(false) || !ofs.good()) {
                return BinaryIOResult::StreamFailed;
            }
            return BinaryIOResult::Success;
        } catch (const std::bad_alloc&) {
            throw;
        } catch (const std::exception&) {
            return BinaryIOResult::ArchiveFailed;
        }
    }

    template <typename RTreeType>
    static BinaryIOResult
    loadBinary(RTreeType& tree, const std::string& filename) {
        try {
            std::ifstream ifs(filename, std::ios::binary);
            if (!ifs.is_open()) {
                return BinaryIOResult::OpenFailed;
            }

            boost::archive::binary_iarchive ia(ifs);
            ia >> tree;
            if (ifs.bad()) {
                return BinaryIOResult::StreamFailed;
            }
            return BinaryIOResult::Success;
        } catch (const std::bad_alloc&) {
            throw;
        } catch (const std::exception&) {
            return BinaryIOResult::ArchiveFailed;
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
