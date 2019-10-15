// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Core/Common/WorkSpacePool.h"

using namespace SPTAG;
using namespace SPTAG::COMMON;


WorkSpacePool::WorkSpacePool(int p_maxCheck, SizeType p_vectorCount)
    : m_maxCheck(p_maxCheck),
      m_vectorCount(p_vectorCount)
{
}


WorkSpacePool::~WorkSpacePool()
{
    for (auto& workSpace : m_workSpacePool)
        workSpace.reset();
    m_workSpacePool.clear();
}


std::shared_ptr<WorkSpace>
WorkSpacePool::Rent()
{
    std::shared_ptr<WorkSpace> workSpace;

    {
        std::lock_guard<std::mutex> lock(m_workSpacePoolMutex);
        if (!m_workSpacePool.empty())
        {
            workSpace = m_workSpacePool.front();
            m_workSpacePool.pop_front();
        }
        else
        {
            workSpace.reset(new WorkSpace);
            workSpace->Initialize(m_maxCheck, m_vectorCount);
        }
    }
    return workSpace;
}


void
WorkSpacePool::Return(const std::shared_ptr<WorkSpace>& p_workSpace)
{
    {
        std::lock_guard<std::mutex> lock(m_workSpacePoolMutex);
        m_workSpacePool.push_back(p_workSpace);
    }
}


void
WorkSpacePool::Init(int size)
{
    for (int i = 0; i < size; i++) 
    {
        std::shared_ptr<WorkSpace> workSpace(new WorkSpace);
        workSpace->Initialize(m_maxCheck, m_vectorCount);
        m_workSpacePool.push_back(std::move(workSpace));
    }
}