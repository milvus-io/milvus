// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_CONCURRENTSET_H_
#define _SPTAG_HELPER_CONCURRENTSET_H_

#include <shared_mutex>
#include <unordered_set>

namespace SPTAG
{
    namespace Helper
    {
        namespace Concurrent
        {
            template <typename T>
            class ConcurrentSet
            {
            public:
                ConcurrentSet();

                ~ConcurrentSet();

                size_t size() const;

                bool contains(const T& key) const;

                void insert(const T& key);

                std::shared_timed_mutex& getLock();

                bool save(std::ostream& output);

                bool save(std::string filename);

                bool load(std::string filename);

                bool load(char* pmemoryFile);

                std::uint64_t bufferSize() const;

            private:
                std::unique_ptr<std::shared_timed_mutex> m_lock;
                std::unordered_set<T> m_data;
            };

            template<typename T>
            ConcurrentSet<T>::ConcurrentSet()
            {
                m_lock.reset(new std::shared_timed_mutex);
            }

            template<typename T>
            ConcurrentSet<T>::~ConcurrentSet()
            {
            }

            template<typename T>
            size_t ConcurrentSet<T>::size() const
            {
                std::shared_lock<std::shared_timed_mutex> lock(*m_lock);
                return m_data.size();
            }

            template<typename T>
            bool ConcurrentSet<T>::contains(const T& key) const
            {
                std::shared_lock<std::shared_timed_mutex> lock(*m_lock);
                return (m_data.find(key) != m_data.end());
            }

            template<typename T>
            void ConcurrentSet<T>::insert(const T& key)
            {
                std::unique_lock<std::shared_timed_mutex> lock(*m_lock);
                m_data.insert(key);
            }

            template<typename T>
            std::shared_timed_mutex& ConcurrentSet<T>::getLock()
            {
                return *m_lock;
            }

            template<typename T>
            std::uint64_t ConcurrentSet<T>::bufferSize() const
            {
                return sizeof(SizeType) + sizeof(T) * m_data.size();
            }

            template<typename T>
            bool ConcurrentSet<T>::save(std::ostream& output)
            {
                SizeType count = (SizeType)m_data.size();
                output.write((char*)&count, sizeof(SizeType));
                for (auto iter = m_data.begin(); iter != m_data.end(); iter++)
                    output.write((char*)&(*iter), sizeof(T));
                std::cout << "Save DeleteID (" << count << ") Finish!" << std::endl;
                return true;
            }

            template<typename T>
            bool ConcurrentSet<T>::save(std::string filename)
            {
                std::cout << "Save DeleteID To " << filename << std::endl;
                std::ofstream output(filename, std::ios::binary);
                if (!output.is_open()) return false;
                save(output);
                output.close();
                return true;
            }

            template<typename T>
            bool ConcurrentSet<T>::load(std::string filename)
            {
                std::cout << "Load DeleteID From " << filename << std::endl;
                std::ifstream input(filename, std::ios::binary);
                if (!input.is_open()) return false;

                SizeType count;
                T ID;
                input.read((char*)&count, sizeof(SizeType));
                for (SizeType i = 0; i < count; i++)
                {
                    input.read((char*)&ID, sizeof(T));
                    m_data.insert(ID);
                }
                input.close();
                std::cout << "Load DeleteID (" << count << ") Finish!" << std::endl;
                return true;
            }

            template<typename T>
            bool ConcurrentSet<T>::load(char* pmemoryFile)
            {
                SizeType count;
                count = *((SizeType*)pmemoryFile);
                pmemoryFile += sizeof(SizeType);

                m_data.insert((T*)pmemoryFile, ((T*)pmemoryFile) + count);
                pmemoryFile += sizeof(T) * count;
                std::cout << "Load DeleteID (" << count << ") Finish!" << std::endl;
                return true;
            }
        }
    }
}
#endif // _SPTAG_HELPER_CONCURRENTSET_H_