// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_COMMON_FINEGRAINEDLOCK_H_
#define _SPTAG_COMMON_FINEGRAINEDLOCK_H_

#include <vector>
#include <mutex>
#include <memory>

namespace SPTAG
{
    namespace COMMON
    {
        class FineGrainedLock {
        public:
            FineGrainedLock() {}
            ~FineGrainedLock() { 
                for (size_t i = 0; i < locks.size(); i++)
                    locks[i].reset();
                locks.clear();
            }
            
            void resize(SizeType n) {
                SizeType current = (SizeType)locks.size();
                if (current <= n) {
                    locks.resize(n);
                    for (SizeType i = current; i < n; i++)
                        locks[i].reset(new std::mutex);
                }
                else {
                    for (SizeType i = n; i < current; i++)
                        locks[i].reset();
                    locks.resize(n);
                }
            }

            std::mutex& operator[](SizeType idx) {
                return *locks[idx];
            }

            const std::mutex& operator[](SizeType idx) const {
                return *locks[idx];
            }
        private:
            std::vector<std::shared_ptr<std::mutex>> locks;
        };
    }
}

#endif // _SPTAG_COMMON_FINEGRAINEDLOCK_H_