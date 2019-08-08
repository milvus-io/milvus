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
                for (int i = 0; i < locks.size(); i++)
                    locks[i].reset();
                locks.clear();
            }
            
            void resize(int n) {
                int current = (int)locks.size();
                if (current <= n) {
                    locks.resize(n);
                    for (int i = current; i < n; i++)
                        locks[i].reset(new std::mutex);
                }
                else {
                    for (int i = n; i < current; i++)
                        locks[i].reset();
                    locks.resize(n);
                }
            }

            std::mutex& operator[](int idx) {
                return *locks[idx];
            }

            const std::mutex& operator[](int idx) const {
                return *locks[idx];
            }
        private:
            std::vector<std::shared_ptr<std::mutex>> locks;
        };
    }
}

#endif // _SPTAG_COMMON_FINEGRAINEDLOCK_H_